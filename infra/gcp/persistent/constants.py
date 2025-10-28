"""Constants shared across modules."""

# Standard library imports
import os
import pathlib

# Declare filepaths
ROOT_DIR = pathlib.Path(__file__).resolve().parents[3]
SERVICES_DIR = ROOT_DIR / "services"
EXTRACT_DIR = SERVICES_DIR / "extract"
TRANSFORM_DIR = SERVICES_DIR / "transform"
MAPPING_DIR = SERVICES_DIR / "map"

# Declare constants
DJANGO_ALLOWED_HOST = ".run.app"
DJANGO_API_PATH_DATA_EXTRACTION = "api/v1/gcp/extract"
DJANGO_SETTINGS_MODULE = "config.settings"
QUEUE_CONFIG = [
    {
        "source": "adb",
        "max_concurrent_dispatches": 2,
        "requires_chromium": False,
    },
    {
        "source": "afdb",
        "max_concurrent_dispatches": 2,
        "requires_chromium": True,
    },
    {
        "source": "aiib",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "bio",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "deg",
        "max_concurrent_dispatches": 1,
        "requires_chromium": False,
    },
    {
        "source": "dfc",
        "max_concurrent_dispatches": 1,
        "requires_chromium": False,
    },
    {
        "source": "ebrd",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "eib",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "fmo",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "idb",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "ifc",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "kfw",
        "max_concurrent_dispatches": 1,
        "requires_chromium": False,
    },
    {
        "source": "miga",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "nbim",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "pro",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "undp",
        "max_concurrent_dispatches": 5,
        "requires_chromium": False,
    },
    {
        "source": "wb",
        "max_concurrent_dispatches": 1,
        "requires_chromium": False,
    },
]

# Parse environment variables
try:
    DJANGO_PORT = os.environ["DJANGO_PORT"]
    DJANGO_SECRET_KEY = os.environ["DJANGO_SECRET_KEY"]
    ENV = "p" if os.environ["ENV"] == "prod" else "t"
    EXTRACTION_PIPELINE_MAX_RETRIES = os.environ[
        "EXTRACTION_PIPELINE_MAX_RETRIES"
    ]
    EXTRACTION_PIPELINE_MAX_WAIT = os.environ["EXTRACTION_PIPELINE_MAX_WAIT"]
    EXTRACTION_PIPELINE_POLLING_INTERVAL = os.environ[
        "EXTRACTION_PIPELINE_POLLING_INTERVAL"
    ]
    EXTRACTION_PIPELINE_SCHEDULE = os.environ["EXTRACTION_PIPELINE_SCHEDULE"]
    GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
    POSTGRES_DB = os.environ["POSTGRES_DB"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    PROJECT_REGION = os.environ["GCP_PROJECT_REGION"]
except KeyError as e:
    raise RuntimeError(f"Missing expected environment variable. {e}")

# Create derived variables
IS_TEST = ENV == "t"
