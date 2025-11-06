"""Utilities for interfacing with remote storage providers."""

# Standard library imports
import os

# Third-party imports
import boto3
import botocore


def configure_cloudflare_request_params() -> dict:
    """Composes request parameters for operations against Cloudflare R2.

    The parameters contain an S3-compatible storage bucket client with
    auth credentials and max retry settings as well as cache control
    headers to set with every request.

    References:
    - https://developers.cloudflare.com/r2/examples/aws/boto3/
    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

    Args:
        `None`

    Returns:
        The Cloudflare R2 request parameters.
    """
    # Parse environment variables
    try:
        cloudflare_access_key_id = os.environ["CLOUDFLARE_R2_ACCESS_KEY_ID"]
        cloudflare_r2_endpoint_url = os.environ["CLOUDFLARE_R2_ENDPOINT_URL"]
        cloudflare_secret_access_key = os.environ["CLOUDFLARE_R2_SECRET_ACCESS_KEY"]
        output_file_max_age = int(os.getenv("OUTPUT_FILE_MAX_AGE", "3600"))
        output_file_max_attempts = int(os.getenv("OUTPUT_FILE_TOTAL_MAX_ATTEMPTS", "3"))
    except KeyError as e:
        raise RuntimeError(
            f"An unexpected error occurred. Cannot find environment variable. {e}"
        ) from None
    except ValueError as e:
        raise RuntimeError(
            f"An unexpected error occurred. Cannot parse environment variable. {e}"
        ) from None

    # Configure new client for writing to S3-compatible storage bucket
    try:
        config = botocore.client.Config(
            tcp_keepalive=True,
            retries={
                "mode": "adaptive",
                "total_max_attempts": output_file_max_attempts,
            },
        )
        session = boto3.Session(
            aws_access_key_id=cloudflare_access_key_id,
            aws_secret_access_key=cloudflare_secret_access_key,
        )
        client = session.client(
            "s3",
            endpoint_url=cloudflare_r2_endpoint_url,
            config=config,
        )
    except Exception as e:
        raise RuntimeError(
            "Failed to configure new client for writing "
            f"to S3-compatible storage bucket. {e}"
        ) from None

    # Compose and return params
    return {
        "client": client,
        "headers": f"public, max-age={output_file_max_age}, must-revalidate",
    }
