"""Loads local, dev, or production configuration
settings for the Django project based on the
value of the "ENV" environmental variable.
Defaults to "dev" if no variable is defined.
"""

import os

env = os.getenv("ENV", "dev")

if env == "local":
    from .settings_local import *
elif env == "dev":
    from .settings_dev import *
elif env == "prod":
    from .settings_prod import *
else:
    raise ValueError(
        f"An invalid environment variable was received: \"{env}\"."
    )