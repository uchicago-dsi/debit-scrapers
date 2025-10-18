"""Classes used to truncate all database tables."""

# Third-party imports
from django.core.management.base import BaseCommand

# Application imports
from common.logger import LoggerFactory
from extract.models import ExtractionJob


class Command(BaseCommand):
    """The Django management command."""

    help_text = "Truncates all database tables."

    def handle(self, **options) -> None:
        """Truncates all tables in the database.

        NOTE: Django will perform a cascading delete
        based on the database model definition.

        Args:
           **options: A dictionary of the command's positional
                and optional arguments.

        Returns:
            `None`
        """
        # Configure logger
        logger = LoggerFactory.get("TRUNCATE")

        # Attempt truncation
        try:
            logger.info("Truncating all database tables.")
            num_deleted, _ = ExtractionJob.objects.all().delete()
            logger.info(f"{num_deleted:,} total record(s) deleted across tables.")
        except Exception as e:
            logger.error(e)
            exit(1)

        # Log success
        logger.info("Database tables truncated successfully.")
