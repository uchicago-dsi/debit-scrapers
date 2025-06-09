"""A Google Cloud Run service to clean and standardize
project, country, and form data. Triggered by a PubSub
message. Written to be idempotent.

References:
- https://cloud.google.com/storage/docs/pubsub-notifications
- https://cloud.google.com/run/docs/samples/cloudrun-pubsub-handler#cloudrun_pubsub_handler-python
- https://cloud.google.com/pubsub/docs/push#receiving_messages
"""

import base64
import flask
import json
import os
from pipeline.constants import DEFAULT_HOST, DEFAULT_PORT, DEV, ENV
from pipeline.services.logger import LoggerFactory
from pipeline.transform.jobs import TransformJobHandler
from werkzeug.exceptions import BadRequest, HTTPException


#region ----------------SETUP---------------------

# Global scope. Runs at GCR instance cold-start
# and may be used in subsequent invocations.

env = os.getenv(ENV, DEV)
logger = LoggerFactory.get("transform-data")
transform_job_handler = TransformJobHandler(logger)
app = flask.Flask(__name__)

#endregion ----------------SETUP---------------------


@app.route("/", methods=["POST"])
def process_request() -> flask.Response:
    """Receives a Pub/Sub push notification to 
    transform staged database records.
    """
    # Initialize error message prefix for logging
    err_msg_prefix = "Failed to parse incoming message."

    # Parse HTTP request body
    try:
        logger.info("Received request to clean records.")
        envelope = flask.request.get_json(force=True)
        logger.info(envelope)
    except json.JSONDecodeError as e:
        raise BadRequest(f"{err_msg_prefix} JSON could not "
                            f"be extracted from request. {e}")

    # Log and confirm existence of "envelope" containing Pub/Sub message
    if not envelope:
        raise BadRequest(f"{err_msg_prefix} No Pub/Sub message received.")

    # Validate format of "envelope"
    if not isinstance(envelope, dict) or "message" not in envelope:
        raise BadRequest(f"{err_msg_prefix} Invalid Pub/Sub "
                            "message format.")

    # Extract pipeline job id from encoded Pub/Sub message data
    try:
        decoded_str = base64.b64decode(envelope["message"]["data"])
        job_id = json.loads(decoded_str)["job_id"]
        logger.info(f"Request for pipeline job \"{job_id}\".")
    except KeyError:
        raise BadRequest(f"{err_msg_prefix} Pub/Sub message "
            "missing \"jobId\" attribute.")
    
    # Transform data corresponding to job id
    success_msg = transform_job_handler.handle(job_id)
    return success_msg, 201


@app.errorhandler(HTTPException)
def handle_error(e: HTTPException):
    """Handles errors that occur during the data transformation
    process by returning either a "400 - Bad Request" or 
    "500 - Internal Server Error" response.
    """
    if HTTPException is BadRequest:
        return e.description, 400
    else:
        return e.description, 500


if __name__ == "__main__":
    debug = env == DEV
    host = os.environ.get("HOST", DEFAULT_HOST)
    port = int(os.environ.get("PORT", "5075"))
    app.run(debug=debug, host=host, port=port)   