"""API views used throughout the application."""

# Standard library imports
import json

# Third-party imports
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View

# Application imports
from common.http import DataRequestClient
from common.tasks import TaskQueueFactory
from extract.dal import DatabaseClient
from extract.workflows.registry import WorkflowClassRegistry

# Instantiate global variables
db_client = DatabaseClient()
data_request_client = DataRequestClient()
queue_client = TaskQueueFactory.get()


@method_decorator(csrf_exempt, name="dispatch")
class GoogleCloudTasksView(View):
    """An API for processing data extraction tasks on Google Cloud Platform."""

    def post(self, request: HttpRequest) -> JsonResponse:
        """Processes a data extraction request triggered by Google Cloud Tasks.

        References:
        - https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler

        Args:
            request: The HTTP request object.

        Returns:
            A JSON response indicating the status of the task processing.
        """
        # Parse request headers
        try:
            message_id = request.headers["X-CloudTasks-TaskName"]
            num_retries = int(
                request.headers["X-CloudTasks-TaskExecutionCount"]
            )
        except KeyError as e:
            return JsonResponse(
                {"error": f'Missing expected HTTP request header "{e}"'},
                status=400,
            )
        except ValueError as e:
            return JsonResponse(
                {"error": f"Request header could not be parsed. {e}"},
                status=400,
            )

        # Decode and extract message data
        try:
            payload = json.loads(request.body) if request.body else {}
            task_id = payload["id"]
            job_id = payload["job_id"]
            source = payload["source"]
            workflow_type = payload["workflow_type"]
            url = payload["url"]
        except json.JSONDecodeError as e:
            return JsonResponse(
                {"error": f"Unable to parse JSON. {e}"}, status=400
            )
        except KeyError as e:
            return JsonResponse(
                {"error": f'Missing expected request body attribute "{e}"'},
                status=400,
            )

        # Instantiate appropriate workflow class from registry
        try:
            w = WorkflowClassRegistry.get(
                source,
                workflow_type,
                data_request_client,
                queue_client,
                db_client,
            )
        except ValueError as e:
            return JsonResponse(
                {"error": f"Failed to instantiate workflow. {e}"},
                status=400,
            )
        except RuntimeError as e:
            return JsonResponse(
                {"error": f"Failed to instantiate workflow. {e}"},
                status=500,
            )

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
            return JsonResponse(
                {"error": f"Error running workflow. {e}"}, status=500
            )

        return HttpResponse(status=200)
