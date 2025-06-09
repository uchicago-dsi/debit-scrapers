"""Factories for generating mock project data.
"""

import factory
from datetime import date, datetime
from django.utils import timezone
from apps.banks.factories import BankFactory
from apps.countries.factories import CountryFactory
from apps.projects.models import Project, ProjectSector, ProjectCountry
from apps.sectors.factories import SectorFactory


class ProjectFactory(factory.django.DjangoModelFactory):
    """Generates mock projects and their associations.
    """
    class Meta:
        model = Project

    class Params:
        date = factory.Faker(
            "date_between",
            start_date=date(year=1950, month=1, day=1)
        )

    id = factory.Sequence(lambda n: n)
    bank = factory.SubFactory(BankFactory)
    number = factory.Faker("numerify", text="P-#########")
    name = factory.Faker("catch_phrase")
    status = factory.Faker(
        "random_element",
        elements=["In Progress", "Completed"]
    )
    year = factory.LazyAttribute(lambda obj: obj.date.year)
    month = factory.LazyAttribute(lambda obj: obj.date.month)
    day = factory.LazyAttribute(lambda obj: obj.date.day)
    loan_amount = factory.Faker("numerify", text="%######")
    loan_amount_currency = "EUR"
    loan_amount_in_usd = None
    sector_list_raw = "power generation"
    sector_list_stnd = "Energy"
    country_list_raw = "aljeria"
    country_list_stnd = "Algeria"
    companies = factory.Faker("company")
    url = factory.Faker("uri")
    created_at_utc = factory.LazyAttribute(
        lambda _: datetime.now(tz=timezone.utc)
    )
    last_updated_utc = factory.LazyAttribute(
        lambda obj: obj.created_at_utc
    )

class ProjectCountryFactory(factory.django.DjangoModelFactory):
    """Generates mock project-country pairs.
    """
    class Meta:
        model = ProjectCountry

    project = factory.SubFactory(ProjectFactory)
    country = factory.SubFactory(CountryFactory)
    created_at_utc = factory.LazyFunction(datetime.utcnow)

class ProjectSectorFactory(factory.django.DjangoModelFactory):
    """Generates mock project-sector pairs.
    """
    class Meta:
        model = ProjectSector

    project = factory.SubFactory(ProjectFactory)
    sector = factory.SubFactory(SectorFactory)
    created_at_utc = factory.LazyFunction(datetime.utcnow)