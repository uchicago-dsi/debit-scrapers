"""Clients for interfacing with the web."""

# Standard library imports
import json
import random
import time
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, IO, Iterator, List

# Third-party imports
import playwright
import playwright.sync_api
import requests
from django.conf import settings
from requests_html import HTMLSession
from playwright.sync_api import sync_playwright, Page


class DataRequestClient:
    """A wrapper for the `requests` library to rotate HTTP headers
    and add random delays to avoid throttling.
    """

    def __init__(self, user_agent_headers: List[str]) -> None:
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
        custom_headers: Dict = None,
        stream: bool = False,
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
            url, timeout=timeout_in_seconds, headers=headers, stream=stream
        )

    def post(
        self,
        url: str,
        use_random_user_agent: bool = False,
        data: Dict = None,
        json: Dict = None,
        timeout_in_seconds: int = 60,
        custom_headers: Dict = None,
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
        )

    def stream_chunks(
        self, url: str, chunk_size: int = 65536
    ) -> Iterator[bytes]:
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


class HeadlessBrowser:
    """Provides a headless Chrome WebDriver."""

    def __init__(self) -> None:
        """Initializes a new instance of a `HeadlessBrowser`.

        Args:
            user_agent_headers: The user agent headers in HTTP requests.

        Returns:
            `None`
        """
        with open(settings.USER_AGENT_HEADERS_FPATH) as f:
            self._user_agent_headers = json.load(f)

    def download_file(
        self, url: str, action: Callable[[Page], None], timeout: int = 60_000
    ) -> IO[bytes]:
        """Downloads a file hosted at the given URL.

        Args:
            url: The URL to the file to download.

            action: The action to trigger the download.

            timeout: The number of milliseconds to wait for the page to load.

        Returns:
            The downloaded file, saved to a temporary location.
        """
        # Select random user agent header
        user_agent = random.choice(self._user_agent_headers)

        with sync_playwright() as p:
            # Launch headless browser
            browser = p.chromium.launch(headless=True)

            # Open new browser tab and navigate to webpage
            first_page = browser.new_page()
            first_page.set_extra_http_headers({"User-Agent": user_agent})
            first_page.goto(url, timeout=timeout)

            # Trigger download event and note download URL
            with first_page.expect_download(timeout=0) as download_event:
                action(first_page)
                download_url = download_event.value.url
                download_name = download_event.value.suggested_filename

            # Open second browser tab
            second_page = browser.new_page()
            second_page.set_extra_http_headers({"User-Agent": user_agent})

            # Start download context and catch aborted event
            with second_page.expect_download(timeout=0) as download_event:
                try:
                    second_page.goto(download_url, timeout=timeout)
                except playwright.sync_api.Error:
                    pass

            # Create temporary file
            prefix, suffix = download_name.split(".")
            temp_file = NamedTemporaryFile(
                delete=False, prefix=prefix, suffix=f".{suffix}"
            )

            # Close file handle to make writable
            temp_file.close()

            # Save download to temporary file
            download_event.value.save_as(temp_file.name)

            # Return temporary file
            return temp_file

    def get_html(self, url: str) -> str:
        """Retrieves the HTML content of a given URL.

        Args:
            url: The resource identifier.

        Returns:
            The HTML content.
        """
        # Select random user agent header
        user_agent = random.choice(self._user_agent_headers)

        # Use headless browser to fetch HTML
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Set a realistic user agent
            page.set_extra_http_headers({"User-Agent": user_agent})

            # Navigate to the page
            page.goto(url)

            # Wait for content to load
            page.wait_for_load_state("networkidle")

            # Get the page HTML
            return page.content()
