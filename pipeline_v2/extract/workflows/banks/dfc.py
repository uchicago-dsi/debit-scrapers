"""Web scrapers for the U.S. International Development
Finance Corporation (DFC), formally known as the
Overseas Private Invesment Corporation (OPIC). Currently
downloads all projects as JSON from a site endpoint.
"""

# Standard library imports
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
        return "https://www3.dfc.gov/OPICProjects/Home/GetOPICActiveProjectList"

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
            response = requests.post(
                url=self.download_url, json={"key": "value"}, verify=False
            )
            return pd.DataFrame.from_dict(response.json())
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
                url = re.search(r"<a href='(.*)' target", details)
                company = re.search(r"<b>(.*)</b>", details)
                name = re.search(r"(?<=<br /><br />).*$", details)
                return {
                    "url": url.group(1) if url else None,
                    "companies": company.group(1) if company else None,
                    "name": name.group(0) if name else None,
                }

            details_df = df.apply(
                parse_project_details, axis="columns", result_type="expand"
            )
            df = pd.concat([df, details_df], axis=1)

            # Add new columns
            df["bank"] = settings.DFC_ABBREVIATION.upper()
            df["number"] = None
            df["status"] = None
            df["month"] = None
            df["day"] = None
            df["loan_amount"] = df["loan_amount_usd"] = df["OPICCommitment"]
            df["loan_amount_currency"] = "USD"

            # Rename columns
            df = df.rename(
                columns={
                    "Year": "year",
                    "Country": "countries",
                    "ProjectType": "sectors",
                }
            )

            col_mapping = {
                "bank": "object",
                "number": "object",
                "name": "object",
                "status": "object",
                "year": "Int64",
                "month": "Int64",
                "day": "Int64",
                "loan_amount": "Float64",
                "loan_amount_currency": "object",
                "loan_amount_usd": "Float64",
                "sectors": "object",
                "countries": "object",
                "companies": "object",
                "url": "object",
            }

            df = df[col_mapping.keys()].astype(col_mapping)

            # Drop records without a URL
            df = df.query("`url` == `url`")

            # Aggregate project financing records by URL. Loans
            # are summed, and the maximum date/year is used to
            # represent the time of the last update.
            def concatenate_values(group: pd.DataFrame, col_name: str) -> str:
                """Parses unique values from a given Pandas `GroupBy`
                column and sorts them in ascending order. Produces
                a formatted output string with commas as separators.

                Args:
                    group: The group.

                    col_name: The column for which to
                        concatenate values.

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
                return ". ".join(unique_values)

            aggregated_projects = []
            groups = df.groupby("url")
            for name, group in groups:
                first = group.iloc[0]
                aggregated_projects.append(
                    {
                        "bank": first["bank"],
                        "number": first["number"],
                        "name": concatenate_values(group, "name"),
                        "status": first["status"],
                        "year": group["year"].max(),
                        "month": first["month"],
                        "day": first["day"],
                        "loan_amount": group["loan_amount"].sum(),
                        "loan_amount_currency": first["loan_amount_currency"],
                        "sectors": concatenate_values(group, "sectors"),
                        "countries": concatenate_values(group, "countries"),
                        "companies": concatenate_values(group, "companies"),
                        "url": name,
                    }
                )

            return pd.DataFrame(aggregated_projects)

        except Exception as e:
            raise Exception(f"Error cleaning DFC projects. {e}")
