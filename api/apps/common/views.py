"""
"""

from django.http import JsonResponse
from .functions import bulk_insert_records, bulk_upsert_records
from rest_framework import generics
from rest_framework.request import Request


class BulkCreateOrIgnoreMixin:
    """A mixin for creating database records in batch while ignoring conflicts.
    """

    def post(self, request: Request, *args, **kwargs):
        """Bulk inserts one or more records into the database
        while ignoring any conflicts that occur.

        Args:
            request (`Request`): The Django REST Framework
                request object.

        Returns:
            (`JsonResponse`): The list of primary keys from
                the newly-created records.
        """
        # Raise exception if no project-country pairs provided
        if not request.data or not isinstance(request.data, list):
            raise ValueError("Expected to receive a list of records.")
        
        # Perform bulk insert of records
        dest_table_name = self.serializer_class.Meta.model._meta.db_table
        field_names = self.serializer_class.Meta.fields
        ids = bulk_insert_records(
            records=request.data,
            dest_table_name=dest_table_name,
            dest_table_fields=field_names
        )

        # Compose return payload and status
        return JsonResponse(ids, status=200, safe=False)

class BulkDeleteMixin:
    """A mixin for deleting database records in batch.
    """

    def delete(self, request: Request) -> JsonResponse:
        """Deletes one or more records from the database.

        Args:
            request (`Request`): The Django REST Framework
                request object. Must contain a list of one 
                or more ids in the request body.

        Returns:
            (`JsonResponse`): A response containing the 
                number of records successfully deleted.
        """
        # Extract ids of records to delete from request payload
        ids = request.data.get("ids")

        # Return error if no ids provided
        if not ids:
            raise ValueError("Expected one or more ids.")

        # Otherwise, filter staged investments by ids and delete
        manager = self.serializer_class.Meta.model.objects
        num_deleted, _ = manager.filter(id__in=ids).delete()

        return JsonResponse(num_deleted, status=200, safe=False)

class CreateOrIgnoreMixin:
    """A mixin for creating a single database record while ignoring conflicts.
    """

    def post(self, request: Request):
        """Uses the view's configured `CreateOrIgnoreSerializer'
        to validate a record before attempting to insert it into
        the database. If a record with the serializer's specified 
        lookup values already exists within the database, then
        that record used for validation and saving instead.

        Args:
            request (`Request`): The Django REST Framework 
                request object.

        Returns:
            (`JsonResponse`): The newly-created data, or the
                data that was previously created.
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        ok_status = 200 if serializer.already_exists else 201
        return JsonResponse(serializer.data, status=ok_status)
