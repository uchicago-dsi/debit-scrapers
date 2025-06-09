"""Factories for generating mock data for countries and their associations.
"""

import factory
from datetime import datetime
from .models import Country


class CountryFactory(factory.django.DjangoModelFactory):
    """Generates mock countries and their associations.
    """
    class Meta:
        model = Country

    id = factory.Sequence(lambda n: n)
    name = "Aruba"
    iso_code = "ABW"
    geojson = ""
    created_at_utc = factory.LazyFunction(datetime.utcnow)
