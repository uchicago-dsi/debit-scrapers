"""API views for data pipeline jobs and tasks.
"""

from apps.common.views import (
    BulkCreateOrIgnoreMixin,
    BulkDeleteMixin,
    CreateOrIgnoreMixin
)
from django.http.response import HttpResponseServerError
from django.http import JsonResponse
from .models import Job, StagedEquityInvestment, StagedProject, Task
from rest_framework.decorators import action
from rest_framework.request import Request
from rest_framework.views import APIView
from rest_framework import viewsets
from .serializers import (
    JobSerializer, 
    StagedEquityInvestmentSerializer,
    StagedProjectSerializer,
    TaskSerializer
)

DEFAULT_RECORDS_PER_PAGE = 1000
MAX_RECORDS_PER_PAGE = 5000
DEFAULT_PAGE = 1


from rest_framework.mixins import (
    ListModelMixin, 
    RetrieveModelMixin,
    UpdateModelMixin
)
from rest_framework.viewsets import GenericViewSet

class JobApiView(
    CreateOrIgnoreMixin,
    ListModelMixin,
    RetrieveModelMixin,
    UpdateModelMixin,
    GenericViewSet):
    """REST API operations for pipeline jobs.
    """
    http_method_names = ["get", "patch", "post"]
    queryset = Job.objects.all()
    serializer_class = JobSerializer

class TaskApiView(
    BulkCreateOrIgnoreMixin,
    ListModelMixin,
    RetrieveModelMixin,
    UpdateModelMixin,
    GenericViewSet):
    """REST API operations for pipeline tasks.
    """
    http_method_names = ["get", "patch", "post"]
    queryset = Task.objects.all()
    serializer_class = TaskSerializer

class StagedEquityInvestmentApiView(
    BulkCreateOrIgnoreMixin,
    ListModelMixin,
    BulkDeleteMixin,
    GenericViewSet):
    """REST API operations for staged equity investments.
    """
    queryset = StagedEquityInvestment.objects.all().order_by("id")
    serializer_class = StagedEquityInvestmentSerializer

class StagedEquityInvestmentListApiView(APIView):
    """Defines permitted operations for collections of staged investments.
    """
    

    def get(self, request: Request) -> JsonResponse:
        """Retrieves staged investments from the database table.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized,
                newly-created staged investment records.
        """
        try:
            # Determine number of records to retrieve
            try:
                limit = int(request.GET.get("limit"))
                limit = DEFAULT_RECORDS_PER_PAGE if \
                    limit > MAX_RECORDS_PER_PAGE else limit
            except (TypeError, ValueError):
                limit = DEFAULT_RECORDS_PER_PAGE

            # Subset and return data
            investments = (StagedEquityInvestment
                           .objects
                           .all()
                           .order_by("id")[:limit])
            serializer = StagedEquityInvestmentSerializer(
                instance=investments,
                many=True)
            return JsonResponse(serializer.data, status=200, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Error fetching staged investments from database. {e}",
                status=500,
                safe=False
            )

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more staged investment records into the database.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (JsonResponse): The list of serialized, newly-created
                staged investment records.
        """
        try:
            # Extract records from request payload
            batch_size = request.data.get("batch_size", 1000)
            investments = request.data.get("records", None)

            # Return error if no investments provided
            if not investments:
                return JsonResponse(
                    data="Expected to receive one or more staged investments.",
                    status=400,
                    safe=False)

            # Serialize data
            is_list = isinstance(investments, list)
            serializer = StagedEquityInvestmentSerializer(
                data=investments,
                many=is_list,
                context={
                    "batch_size": batch_size,
                    "create": True,
                    "ignore_conflicts": True
                }
            )

            # Return serializer error(s) if data invalid
            if not serializer.is_valid():
                return JsonResponse(serializer.errors, status=400, safe=False)

            # Otherwise, save and return newly-created records
            serializer.save()
            return JsonResponse(serializer.data, status=201, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Failed to insert staged investments in database. {e}",
                status=500,
                safe=False
            )

class StagedProjectListApiView(APIView):
    """Defines permitted operations for collections of staged projects.
    """

    def delete(self, request: Request) -> JsonResponse:
        """Deletes one or more staged project records from the database by id.

        Args:
            request (`Request`): The Django REST Framework request object.
                Must contain a list of one or more ids in the request body.

        Returns:
            (`JsonResponse`): A response containing the number of records
                successfully deleted.
        """
        try:
            # Extract ids of records to delete from request payload
            ids = request.data.get("ids")

            # Return error if no ids provided
            if not ids:
                return JsonResponse(
                    data="Expected to receive one or more staged project ids.",
                    status=400,
                    safe=False)

            # Otherwise, filter staged projects by ids and delete
            num_deleted, _ = StagedProject.objects.filter(id__in=ids).delete()

            return JsonResponse(num_deleted, status=200, safe=False)

        except Exception as e:
            return HttpResponseServerError("Failed to delete "
                f"staged records from database. {e}")

    def get(self, request: Request) -> JsonResponse:
        """Retrieves staged projects from the database table.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized, newly-created
                staged project records.
        """
        try:
            # Determine number of records to retrieve
            try:
                limit = int(request.GET.get("limit"))
                limit = DEFAULT_RECORDS_PER_PAGE if limit > MAX_RECORDS_PER_PAGE else limit
            except (TypeError, ValueError):
                limit = DEFAULT_RECORDS_PER_PAGE

            # Subset and return data
            staged_projects = StagedProject.objects.all().order_by("id")[:limit]
            serializer = StagedProjectSerializer(staged_projects, many=True)
            return JsonResponse(serializer.data, status=200, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Failed to retrieve staged projects from database. {e}",
                status=500,
                safe=False
            )

    def patch(self, request: Request) -> JsonResponse:
        """Updates one or more staged project records from the database by id.

        Args:
            request (`Request`): The Django REST Framework request object.
                Must contain a list of one or more ids in the request body.

        Returns:
            (`JsonResponse`): A response containing the number of records
                successfully deleted.
        """
        try:
            # Parse request body to confirm existence of 
            # unique project fields (aside from "id")
            try:
                bank = request.data["bank"]
                url = request.data["url"]
            except KeyError:
                return JsonResponse(
                    data=(
                        "Missing required fields \"bank\" and \"url\" " 
                        "in HTTP request body."
                    ),
                    status=400,
                    safe=False
                )
            
            # Retrieve corresponding bank by fields
            try:
                project = StagedProject.objects.get(bank=bank, url=url)
            except StagedProject.DoesNotExist:
                return JsonResponse(
                    data="Requested staged project does not exist.",
                    status=404
                )
            
            # Perform update if other project fields are valid
            serializer = StagedProjectSerializer(
                project,
                data=request.data,
                partial=True
            )
            if serializer.is_valid():
                serializer.save()
                return JsonResponse(serializer.data, status=200, safe=False)

            return JsonResponse(serializer.errors, status=400, safe=False)

        except Exception as e:
            return JsonResponse(
                data=(
                    f"Failed to update project with for bank \"{bank}\" "
                    f"at url \"{url}\". {e}"
                ),
                status=500,
                safe=False
            )

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more staged project records into the database.

        Args:
            request (`Request`): The request object.

        Returns:
            (`JsonResponse`): The list of serialized, newly-created
                staged project records.
        """
        try:
            # Extract records from request payload
            batch_size = request.data.get("batch_size", 1000)
            projects = request.data.get("records", None)

            # Return error if no projects provided
            if not projects:
                return JsonResponse(
                    data="Expected to receive one or more staged projects.",
                    status=400,
                    safe=False)

            # Serialize data
            is_list = isinstance(projects, list)
            serializer = StagedProjectSerializer(
                data=projects,
                many=is_list,
                context={
                    "batch_size": batch_size,
                    "create": True,
                    "ignore_conflicts": True
                })

            # Return serializer error(s) if data invalid
            if not serializer.is_valid():
                return JsonResponse(serializer.errors, status=400, safe=False)

            # Otherwise, save and return newly-created records
            serializer.save()
            return JsonResponse(serializer.data, status=201, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Failed to insert staged projects in database. {e}",
                status=500,
                safe=False
            )

class StagedProjectByUrlViewSet(viewsets.GenericViewSet):
    """Defines permitted operations for modifying staged projects by URL.
    """
    
    @action(detail=False, methods=["delete", "post"], url_path="delete")
    def delete(self, request: Request) -> JsonResponse:
        """Deletes one or more staged project records from the database by url.

        Args:
            request (`Request`): The Django REST Framework request object.
                Must contain a list of one or more ids in the request body.

        Returns:
            (`JsonResponse`): A response containing the number of records
                successfully deleted.
        """
        try:
            # Extract urls of records to delete from request payload
            urls = request.data.get("urls")

            # Return error if no ids provided
            if not urls:
                return JsonResponse(
                    data="Expected to receive one or more staged project urls.",
                    status=400,
                    safe=False)

            # Otherwise, filter staged projects by ids and delete
            num_deleted, _ = StagedProject.objects.filter(url__in=urls).delete()

            return JsonResponse(num_deleted, status=200, safe=False)

        except Exception as e:
            return HttpResponseServerError(
                f"Failed to delete staged records from database. {e}"
            )

    @action(detail=False, methods=["post"], url_path="search")
    def search(self, request: Request) -> JsonResponse:
        """Retrieves staged projects from the database table by url.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized staged project records.
        """
        try:
            # Extract urls of records to retrieve from request payload
            urls = request.data.get("urls")

            # Return error if no ids provided
            if not urls:
                return JsonResponse(
                    data="Expected to receive one or more staged project urls.",
                    status=400,
                    safe=False)

            # Otherwise, filter staged projects by urls
            staged_projects = StagedProject.objects.filter(url__in=urls)
            serializer = StagedProjectSerializer(staged_projects, many=True)

            # Return data
            return JsonResponse(serializer.data, status=200, safe=False)

        except Exception as e:
            return HttpResponseServerError("Failed to delete "
                f"staged records from database. {e}")

