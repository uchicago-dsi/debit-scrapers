"""Custom middleware used throughout the Django project."""

# Standard library imports
from collections.abc import Callable

# Third-party imports
from django.http import HttpRequest, HttpResponse


class HealthCheckMiddleware:
    """Middleware used to validate the health of a running Django server.

    External services can call a `/` endpoint even if their host/domain
    names are not declared in the `ALLOWED_HOSTS` Django setting. This is
    necessary to permit cloud providers to perform health checks against
    deployed Django servers with a dynamically-generated IP address.

    See: https://stackoverflow.com/a/64623669
    """

    def __init__(self, get_response: Callable) -> None:
        """Initializes a new instance of `HealthCheckMiddleware`.

        Args:
            get_response: A callable that returns an HTTP response.

        Returns:
            `None`
        """
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        """Handles incoming HTTP requests.

        Returns a "200 - OK" HTTP response for calls made to the "/"
        endpoint and forwards the request to the next middleware otherwise.

        Args:
            request: The incoming HTTP request.

        Returns:
            The HTTP response.
        """
        if request.path == "/":
            return HttpResponse("ok")
        return self.get_response(request)
