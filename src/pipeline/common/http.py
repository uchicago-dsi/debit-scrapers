"""Classes for issuing HTTP requests."""

# Standard library imports
import random
import time
from collections.abc import Iterator

# Third-party imports
import requests
from requests_html import HTMLSession


class DataRequestClient:
    """A wrapper for the `requests` library."""

    def __init__(self, user_agent_headers: list[str]) -> None:
        """Initializes a new instance of a `DataRequestClient`.

        Args:
            user_agent_headers: The user agent headers in HTTP requests.

        Returns:
            `None`
        """
        self._user_agent_headers = user_agent_headers

    def get(
        self,
        url: str,
        use_random_user_agent: bool = False,
        use_random_delay: bool = False,
        min_random_delay: int = 1,
        max_random_delay: int = 3,
        timeout_in_seconds: int = 60,
        custom_headers: dict = None,
        stream: bool = False,
        verify: bool = True,
    ) -> requests.Response:
        """Makes an HTTP GET request against the given URL.

        Args:
            url: The resource identifier.

            use_random_user_agent: A boolean indicating
                whether one of several user agent HTTP headers
                should be randomly selected and included.
                Defaults to False.

            use_random_delay: A boolean indicating how
                whether a random delay should be added before
                making the requests. Defaults to False.

            min_random_delay: The minimum number of seconds
                that should be included in a random delay.
                Defaults to 1.

            max_random_delay: The maximum number of seconds
                that should be included in a random delay.
                Defaults to 3.

            timeout_in_seconds: The number of seconds the
                request should be awaited before raising a timeout
                error. Defaults to 60. A value of `None` will cause
                the request to wait indefinitely.

            custom_headers: Custom headers to include in the request.

            stream: A boolean indicating whether the response
                should be streamed. Defaults to False.

            verify: A boolean indicating whether the SSL
                certificate should be verified. Defaults to True.

        Returns:
            The response object.
        """
        # Implement random delay
        if max_random_delay < min_random_delay:
            raise ValueError(
                "The minimum delay time must be less than the maximum time."
            )
        if use_random_delay:
            delay = random.randint(min_random_delay, max_random_delay)
            time.sleep(delay)

        # Initialize HTTP headers
        headers = {}
        if use_random_user_agent:
            agent_idx = random.randint(0, len(self._user_agent_headers) - 1)
            headers["User-Agent"] = self._user_agent_headers[agent_idx]
        if custom_headers:
            headers = {**headers, **custom_headers}

        # Initialize session
        s = HTMLSession()

        # Fetch data
        return s.get(
            url,
            timeout=timeout_in_seconds,
            headers=headers,
            stream=stream,
            verify=verify,
        )

    def post(
        self,
        url: str,
        use_random_user_agent: bool = False,
        data: dict = None,
        json: dict = None,
        timeout_in_seconds: int = 60,
        custom_headers: dict = None,
        verify: bool = True,
    ) -> requests.Response:
        """Makes an HTTP POST request against the given URL.

        Args:
            url: The resource identifier.

            use_random_user_agent: A boolean indicating
                whether one of several user agent HTTP headers
                should be randomly selected and included.
                Defaults to False.

            data: The data to send in the request.

            json: The JSON data to send in the request.

            timeout_in_seconds: The number of seconds the
                request should be awaited before raising a timeout
                error. Defaults to 60. A value of `None` will cause
                the request to wait indefinitely.

            custom_headers: Custom headers to include in the request.

            verify: A boolean indicating whether the SSL
                certificate should be verified. Defaults to True.

        Returns:
            The response object.
        """
        if use_random_user_agent and not custom_headers:
            agent_idx = random.randint(0, len(self._user_agent_headers) - 1)
            headers = {"User-Agent": self._user_agent_headers[agent_idx]}
        elif custom_headers:
            headers = custom_headers
        else:
            headers = None

        s = HTMLSession()

        return s.post(
            url,
            data=data,
            json=json,
            timeout=timeout_in_seconds,
            headers=headers,
            verify=verify,
        )

    def stream_chunks(self, url: str, chunk_size: int = 65536) -> Iterator[bytes]:
        """Streams data from the given URL as chunks.

        Args:
            url: The download link.

            chunk_size: The size of each chunk in bytes. Defaults to 65536.

        Yields:
            A chunk of data until the stream is exhausted.
        """
        downloaded = 0
        while True:
            headers = {"Range": f"bytes={downloaded}-"}
            try:
                with self.get(url, custom_headers=headers, stream=True) as r:
                    if not r.ok:
                        raise RuntimeError(
                            "Error fetching data. The request failed with "
                            f'a "{r.status_code} - {r.reason}" status '
                            f'code and the message "{r.text}".'
                        )

                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            downloaded += len(chunk)
                            yield chunk
                    return
            except (requests.exceptions.ChunkedEncodingError, ConnectionError):
                time.sleep(1)
