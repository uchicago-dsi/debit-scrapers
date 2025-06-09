"""Custom implementations of exception classes and handlers.
"""

from django.http import JsonResponse
from rest_framework.views import exception_handler
from typing import Dict


def custom_exception_handler(exc: Exception, context: Dict):
    """Handles exceptions by returning a response with 
    the appropriate status code and message.
    
    Args:
        exc (`Exception`): The exception object raised.

        context (`dict`): A dictionary containing information about
            the current request and view that raised the exception.
            Contained the keys "view", "args", "kwargs", and "request"
            at the time of writing.

    References:
    - ["Custom Exception Handler in Django Rest Framework"](https://technostacks.com/blog/custom-exception-handler-in-django-rest-framework/)
    - ["Stack Overflow Post"](https://stackoverflow.com/a/30628065)

    Returns:
        (`JsonResponse`): The response object.
    """
    # Pass to Django REST's default exception handler 
    # to get standard error response
    response = exception_handler(exc, context)

    # Parse response for status code
    try:
        status_code = response.status_code
    except:
        status_code = 500

    # Pull error message from underlying API view, if configured
    try:
        error_msg = (context["view"].error_message + " " + str(exc)).strip()
    except:
        error_msg = str(exc)

    return JsonResponse(
        data=error_msg,
        status=status_code,
        safe=False
    )
