"""Web scrapers for the U.S. International Development
Finance Corporation (DFC), formally known as the
Overseas Private Invesment Corporation (OPIC). Currently
downloads all projects as JSON from a site endpoint.
"""

# Standard library imports
import warnings
from logging import Logger

# Third-party imports
import pandas as pd
import re
import requests
from django.conf import settings

# Application imports
from common.web import DataRequestClient
from extract.dal import ExtractionDbClient
from extract.workflows.abstract import ProjectDownloadWorkflow


class DfcDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from DFC's website
    and then cleans and saves the data to a database using
    the `execute` method defined in its superclass.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: ExtractionDbClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `DfcDownloadWorkflow`.

         Args:
            data_request_client: A client for making HTTP GET requests while
                adding random delays and rotating user agent headers.

            db_client: A client used to insert and
                update tasks in the database.

            logger: An standard logger instance.

        Returns:
            `None`
        """
        super().__init__(data_request_client, db_client, logger)

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return (
            "https://www3.dfc.gov/OPICProjects/Home/GetOPICActiveProjectList"
        )

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects as JSON from
        DFC's website. NOTE: The endpoint does not have a valid
        SSL certificate, so verification is turned off for this
        request only.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=requests.packages.urllib3.exceptions.InsecureRequestWarning,
                )
                r = requests.post(url=self.download_url, verify=False)
                return pd.DataFrame.from_dict(r.json())
        except Exception as e:
            raise Exception(
                f"Error retrieving DFC projects from '{self.download_url}' "
                f"and parsing into Pandas DataFrame. {e}"
            )

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DFC project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Parse 'ProjectDetails' HTML column
            def parse_project_details(row: pd.Series):
                """Uses regex to parse the 'ProjectDetails' column and extract
                the project name, company, and URL.

                Args:
                    row: The DataFrame row.

                Returns:
                    The new values and their corresponding keys.
                """
                details = row["ProjectDetails"]

                # Parse URL and project number from anchor tag
                url_match = re.search(r"<a href='(.*)' target", details)
                url = url_match.group(1) if url_match else None
                number = (
                    None if not url else url.replace(".pdf", "").split("/")[-1]
                )

                # Parse project affiliate and name in remaining HTML
                affiliate = re.search(r"<b>(.*)</b>", details)
                name = re.search(r"(?<=<br /><br />).*$", details)

                return {
                    "number": number,
                    "name": name.group(0) if name else None,
                    "affiliates": affiliate.group(1) if affiliate else None,
                    "url": url,
                }

            details_df = df.apply(
                parse_project_details, axis="columns", result_type="expand"
            )
            df = pd.concat([df, details_df], axis=1)

            # Add new columns
            df["source"] = settings.DFC_ABBREVIATION.upper()
            df["total_amount"] = df["total_amount_usd"] = df["OPICCommitment"]
            df["total_amount_currency"] = "USD"

            # Rename columns
            df = df.rename(
                columns={
                    "Year": "year",
                    "Country": "countries",
                    "ProjectType": "finance_types",
                }
            )

            col_mapping = {
                "source": "object",
                "number": "object",
                "name": "object",
                "year": "Int64",
                "total_amount": "Float64",
                "total_amount_currency": "object",
                "total_amount_usd": "Float64",
                "finance_types": "object",
                "countries": "object",
                "affiliates": "object",
                "url": "object",
            }

            df = df[col_mapping.keys()].astype(col_mapping)

            # Drop records without a URL
            df = df.query("`url` == `url`")

            # Aggregate project financing records by URL. Loans
            # are summed, and the maximum date/year is used to
            # represent the time of the last update.
            def concatenate_values(
                group: pd.DataFrame, col_name: str, delimiter: str = "|"
            ) -> str:
                """Parses unique values from a given Pandas `GroupBy`
                column and sorts them in ascending order. Produces
                a formatted output string with commas as separators.

                Args:
                    group: The group.

                    col_name: The column for which to
                        concatenate values.

                    delimiter: The string used to join values.
                        Defaults to a pipe ("|").

                Returns:
                    The concatenated values.
                """
                unique_values = (
                    group[col_name]
                    .apply(lambda val: val[:-1] if val.endswith(".") else val)
                    .sort_values()
                    .unique()
                    .tolist()
                )
                return delimiter.join(unique_values)

            aggregated_projects = []
            groups = df.groupby("url")
            for name, group in groups:
                first = group.iloc[0]
                aggregated_projects.append(
                    {
                        "affiliates": concatenate_values(group, "affiliates"),
                        "countries": concatenate_values(group, "countries"),
                        "date_effective": str(group["year"].min()),
                        "finance_types": concatenate_values(
                            group, "finance_types"
                        ),
                        "name": concatenate_values(group, "name", ". "),
                        "number": first["number"],
                        "source": first["source"],
                        "total_amount": group["total_amount"].sum(),
                        "total_amount_currency": first[
                            "total_amount_currency"
                        ],
                        "url": name,
                    }
                )

            return pd.DataFrame(aggregated_projects)

        except Exception as e:
            raise Exception(f"Error cleaning DFC projects. {e}")
