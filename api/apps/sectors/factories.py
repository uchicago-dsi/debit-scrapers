"""Factories for generating mock data for sectors.
"""

import factory
from datetime import datetime
from .models import Sector


class SectorFactory(factory.django.DjangoModelFactory):
    """Generates mock sectors.
    """
    class Meta:
        model = Sector

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: f"Sector {n}")
    created_at_utc = factory.LazyFunction(datetime.utcnow)
