"""Factories for generating mock pipeline data.
"""

import factory
from datetime import date, datetime, timedelta
from django.utils import timezone
from apps.pipeline.models import Job


class JobFactory(factory.django.DjangoModelFactory):
    """Generates mock jobs.
    """
    class Meta:
        model = Job

    class Params:
        date = factory.Faker(
            "date_between",
            start_date=date(year=1950, month=1, day=1)
        )

    id = factory.Sequence(lambda n: n)
    invocation_id = factory.Sequence(lambda n: n)
    job_type = factory.Faker(
        "random_element", 
        elements=("projects", "form13f")
    )
    data_load_stage = factory.Faker(
        "random_element",
        elements=("Not Started", "In Progress", "Error", "Completed")
    )
    data_load_start_utc = factory.LazyAttribute(
        lambda _: datetime.now(tz=timezone.utc)
    )
    data_load_end_utc = factory.LazyAttribute(
        lambda obj: obj.data_load_start_utc + timedelta(minutes=30)
    )
    data_clean_stage = factory.Faker(
        "random_element",
        elements=("Not Started", "In Progress", "Error", "Completed")
    )
    data_clean_start_utc = factory.LazyAttribute(
        lambda _: datetime.now(tz=timezone.utc)
    )
    data_clean_end_utc = factory.LazyAttribute(
        lambda obj: obj.data_load_start_utc + timedelta(minutes=30)
    )
    raw_url = None
    clean_url = None
    audit_url = None
    created_at_utc = factory.LazyAttribute(
        lambda _: datetime.now(tz=timezone.utc)
    )
