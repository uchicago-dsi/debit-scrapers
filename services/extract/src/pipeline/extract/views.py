"""API views used throughout the application."""

# Standard library imports
import json

# Third-party imports
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views import View

# Application imports
from common.http import DataRequestClient
from common.logger import LoggerFactory
from extract.sql import DatabaseClient
from extract.tasks import TaskQueueFactory
from extract.workflows.registry import WorkflowClassRegistry

# Instantiate global variables
logger = LoggerFactory.get("WORKER - ROUTER")
db_client = DatabaseClient()
data_request_client = DataRequestClient()
queue_client = TaskQueueFactory.get()


@method_decorator(csrf_exempt, name="dispatch")
class ExtractionWorkflowView(View):
    """An API for processing data extraction tasks on Google Cloud Platform."""

    def post(self, request: HttpRequest) -> JsonResponse:
        """Processes a data extraction request triggered by Google Cloud Tasks.

        References:
        - https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler

        Args:
            request: The HTTP request object.

        Returns:
            The HTTP response.
        """
        # Log receipt of request
        logger.info(f"New request received: {json.loads(request.body)}")

        # Parse request headers
        try:
            message_id = request.headers["X-Cloudtasks-Taskname"]
            num_retries = int(request.headers["X-Cloudtasks-Taskretrycount"])
        except KeyError as e:
            err_msg = f'Missing expected HTTP request header "{e}"'
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=400)
        except ValueError as e:
            err_msg = f"Request header could not be parsed. {e}"
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=400)

        # Decode and extract message data
        try:
            payload = json.loads(request.body) if request.body else {}
            task_id = payload["id"]
            job_id = payload["job"]
            source = payload["source"]
            workflow_type = payload["workflow_type"]
            url = payload["url"]
        except json.JSONDecodeError as e:
            err_msg = f"Unable to parse JSON. {e}"
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=400)
        except KeyError as e:
            err_msg = f'Missing expected request body attribute "{e}"'
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=400)

        # Instantiate appropriate workflow class from registry
        try:
            w = WorkflowClassRegistry.get(
                source,
                workflow_type,
                data_request_client,
                queue_client,
                db_client,
            )
        except (ValueError, RuntimeError) as e:
            err_msg = f"Failed to instantiate workflow. {e}"
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=400)

        # Run workflow
        try:
            w.execute(
                message_id,
                num_retries,
                job_id,
                task_id,
                source,
                url,
            )
        except Exception as e:
            err_msg = f"Error running workflow. {e}"
            logger.error(err_msg)
            return JsonResponse({"error": err_msg}, status=500)

        return HttpResponse(status=200)
