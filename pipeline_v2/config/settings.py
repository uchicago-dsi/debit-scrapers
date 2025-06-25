"""Settings used across development environments."""

# Standard library imports
import os
from pathlib import Path

# region DEFAULT SETTINGS

# ________________________________________________________________________
# SERVER
# ________________________________________________________________________

DEBUG = os.environ["ENV"] != "PROD"

# ________________________________________________________________________
# FILE PATHS
# ________________________________________________________________________

BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_STORAGE_DIR = BASE_DIR / "data"
TEMP_DOWNLOAD_DIR = "/tmp"
EXTRACT_DIR = BASE_DIR / "extract"
EXTRACT_CONFIG_DIR = EXTRACT_DIR / "config"
EXTRACT_TEST_DIR = EXTRACT_DIR / "tests"

IATI_ACTIVITY_SECTOR_FPATH = EXTRACT_CONFIG_DIR / "iati_activity_sector_codes.json"
IATI_ACTIVITY_STATUS_FPATH = EXTRACT_CONFIG_DIR / "iati_activity_status_codes.json"
IDB_DOWNLOAD_OPTIONS_FPATH = EXTRACT_CONFIG_DIR / "idb_download_options.json"
USER_AGENT_HEADERS_FPATH = EXTRACT_CONFIG_DIR / "user_agent_headers.json"

# ________________________________________________________________________
# INSTALLED APPS
# ________________________________________________________________________

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "common",
    "extract",
]

# ________________________________________________________________________
# MIDDLEWARE
# ________________________________________________________________________

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# ________________________________________________________________________
# TEMPLATES
# ________________________________________________________________________

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# ________________________________________________________________________
# VIEWS
# ________________________________________________________________________

ROOT_URLCONF = "config.urls"
STATIC_URL = "/static/"
WSGI_APPLICATION = "config.wsgi.application"

# ________________________________________________________________________
# AUTHENTICATION
# ________________________________________________________________________

ALLOWED_HOSTS = [os.environ["DJANGO_ALLOWED_HOST"]]
SECRET_KEY = os.environ["DJANGO_SECRET_KEY"]
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# ________________________________________________________________________
# INTERNATIONALIZATION
# ________________________________________________________________________

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True

# ________________________________________________________________________
# DATABASE
# ________________________________________________________________________

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("POSTGRES_DB", "postgres"),
        "USER": os.getenv("POSTGRES_USER", "postgres"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
        "HOST": os.getenv("POSTGRES_HOST", "postgres"),
        "PORT": int(os.getenv("POSTGRES_PORT", 5432)),
        "CONN_MAX_AGE": int(os.getenv("POSTGRES_CONN_MAX_AGE", 0)),
        "DISABLE_SERVER_SIDE_CURSORS": False,
        "OPTIONS": {"sslmode": "prefer"},
    }
}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ________________________________________________________________________
# LOGGING
# ________________________________________________________________________

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}

# endregion

# region CUSTOM SETTINGS

# ________________________________________________________________________
# WORKFLOW TYPES
# ________________________________________________________________________

DYNAMIC_WORKFLOW = "dynamic"
PROJECT_DOWNLOAD_WORKFLOW = "project-download"
PROJECT_PAGE_WORKFLOW = "project-page-scrape"
PROJECT_PARTIAL_DOWNLOAD_WORKFLOW = "project-partial-download"
PROJECT_PARTIAL_PAGE_WORKFLOW = "project-partial-page-scrape"
RESULTS_PAGE_MULTISCRAPE_WORKFLOW = "results-page-multiscrape"
RESULTS_PAGE_WORKFLOW = "results-page-scrape"
SEED_URLS_WORKFLOW = "seed-urls"

# ________________________________________________________________________
# BANKS
# ________________________________________________________________________

ADB_ABBREVIATION = "adb"
AFDB_ABBREVIATION = "afdb"
AIIB_ABBREVIATION = "aiib"
BIO_ABBREVIATION = "bio"
DEG_ABBREVIATION = "deg"
DFC_ABBREVIATION = "dfc"
EBRD_ABBREVIATION = "ebrd"
EIB_ABBREVIATION = "eib"
FMO_ABBREVIATION = "fmo"
IDB_ABBREVIATION = "idb"
IFC_ABBREVIATION = "ifc"
KFW_ABBREVIATION = "kfw"
MIGA_ABBREVIATION = "miga"
NBIM_ABBREVIATION = "nbim"
PRO_ABBREVIATION = "pro"
UNDP_ABBREVIATION = "undp"
WB_ABBREVIATION = "wb"


# endregion
