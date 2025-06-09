"""Factories for generating mock bank data.
"""

import factory
from datetime import datetime
from .models import Bank

class BankFactory(factory.django.DjangoModelFactory):
    """Generates mock banks and their associations.
    """
    class Meta:
        model = Bank
    
    class Params:
        random_name = factory.Faker("last_name_nonbinary")

    id = factory.Sequence(lambda n: n)
    name = factory.LazyAttribute(lambda obj: obj.random_name + " Bank")
    abbreviation = factory.LazyAttribute(lambda obj: obj.name.lower()[:3])
    created_at_utc = factory.LazyFunction(datetime.utcnow)
