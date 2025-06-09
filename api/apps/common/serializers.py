"""Generic serializers for use throughout the application.
"""

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import IntegrityError, models
from django.db.models import Q
from rest_framework import serializers
from typing import Any


class BulkUpsertSerializer(serializers.ListSerializer):
    """A serializer for bulk creating or updating lists of objects.
    """

    def create(self, validated_data: Any):
        """Bulk inserts or upserts a generic set of objects into their
        corresponding Django model's database table using Django's
        default implementations. The max batch size is 1000.

        Args:
            validated_data (`any`): Data whose schema has been validated.

        Returns:
            (`list` of `any`): The created objects.
        """
        # Extract upsert settings from serializer context
        create = self.context.get("create", True)
        ignore_conflicts = self.context.get("ignore_conflicts", False)
        batch_size = self.context.get("batch_size", 1000)
        fields = self.context.get("fields", None)

        # Raise exception if no model field names are provided for update
        if not create and not fields:
            raise ValueError("No model fields specified for update.")

        # Create model instances from validated data
        result = [self.child.Meta.model(**attrs) for attrs in validated_data]

        # Bulk insert or update model instances in batch
        try:
            if create:
                return self.child.Meta.model.objects.bulk_create(
                    objs=result,
                    ignore_conflicts=ignore_conflicts,
                    batch_size=batch_size
                )
            else:
                return self.child.Meta.model.objects.bulk_update(
                    objs=result,
                    fields=fields,
                    batch_size=batch_size
                )
        except IntegrityError:
            return []

class DynamicFieldsSerializer(serializers.ModelSerializer):
    """A serializer that dynamically sets required fields on a model.
    Adapted from the following resources:

    - http://blog.josephmisiti.com/customzing-apis-with-django-rest-framework
    - https://stackoverflow.com/a/53320202
    """
    
    def __init__(self, *args, **kwargs) -> None:
        """Initializes a new instance of the `DynamicFieldsSerializer`.

        Args:
            `None`

        Returns:
            `None`
        """
        # Don't pass the 'fields' arg up to the superclass
        fields = kwargs.pop('fields', None)

        # Instantiate the superclass normally
        super(DynamicFieldsSerializer, self).__init__(*args, **kwargs)

        if fields:
            # Drop any fields that are not specified in the `fields` argument.
            allowed = set(fields)
            existing = set(self.fields.keys())
            for field_name in existing - allowed:
                self.fields.pop(field_name)



class BulkInsertSerializer(serializers.ModelSerializer):
    """
    """

    def __init__(self, instance:models.Model=None, **kwargs) -> None:
        """Overrides the default initializer to indicate whether
        a validated entity already exists in the database.

        Args:
            instance (`models.Model`): The database model 
                to validate the data against.

        Returns:
            `None`
        """
        self.num_created = self.num_updated = 0
        super().__init__(instance=instance, **kwargs)


    def is_valid(self, raise_exception:bool=False) -> bool:
        """Overrides the `ModelSerializer` super class" validation
        method by querying the database for entities whose given
        lookup values match those of the Django model instances
        currently being validated. If such entities are found, the
        reference to the serializer instances are updated to point
        to the entities. Finally, the entities are validated as normal.

        References:
            - https://stackoverflow.com/a/48310365

        Args:
            raise_exception (`bool`): A boolean indicating
                whether validation failures should result
                in an exception.

        Returns:
            (`bool`): The True or False value.
        """
        if hasattr(self, "initial_data"):
            try:
                # Build query to search for records that share
                # same unique field values as initial data
                query = Q()
                obj_lookup = []
                for obj in self["initial_data"]:
                    filters = {}
                    values = []
                    for field in self.Meta.unique_fields:
                        filters[field] = obj[field]
                        values.append(obj[field])
                    query |= filters
                    obj_lookup.append("-".join(values))

                # Fetch pre-existing records
                to_update = (self.Meta.model.objects
                        .filter(query)
                        .values(fields=self.Meta.unique_fields))
                
                # Determine number of records that will be created and updated
                self.num_updated = len(to_update)
                self.num_created = len(self["initial_data"]) - len(to_update)

                # Create instance
                to_create = []
                for candidate in to_update:
                    


            except (KeyError, ObjectDoesNotExist, MultipleObjectsReturned):
                pass

        return super().is_valid(raise_exception)


class CreateOrIgnoreSerializer(serializers.ModelSerializer):
    """A serializer that handles inserts where conflicts are ignored.
    """

    def __init__(self, instance:models.Model=None, **kwargs) -> None:
        """Overrides the default initializer to indicate whether
        a validated entity already exists in the database.

        Args:
            instance (`models.Model`): The database model 
                to validate the data against.

        Returns:
            `None`
        """
        self.already_exists = None
        super().__init__(instance=instance, **kwargs)

    def is_valid(self, raise_exception:bool=False) -> bool:
        """Overrides the `ModelSerializer` super class" validation
        method by querying the database for an entity whose given
        lookup values match those of the Django model instance
        currently being validated. If such an entity is found, the
        reference to the serializer instance is updated to point
        to the entity. Finally, the instance is validated as normal.

        References:
            - https://stackoverflow.com/a/48310365

        Args:
            raise_exception (`bool`): A boolean indicating
                whether validation failures should result
                in an exception.

        Returns:
            (`bool`): The True or False value.
        """
        self.already_exists = False
        if hasattr(self, "initial_data"):
            try:
                kwargs = {
                    f:self.initial_data[f] 
                    for f in self.Meta.unique_fields
                }
                obj = self.Meta.model.objects.get(**kwargs)
                self.instance = obj
                self.already_exists = True
            except (KeyError, ObjectDoesNotExist, MultipleObjectsReturned):
                pass
        return super().is_valid(raise_exception)
