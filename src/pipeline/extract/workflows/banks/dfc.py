"""U.S. International Development Finance Corporation (DFC)

The organization is formally known as the Overseas Private Investment
Corporation (OPIC). Data is retrieved through a direct download.
"""

# Standard library imports
import re
import warnings

# Third-party imports
import numpy as np
import pandas as pd
import requests
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class DfcDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads and parses a JSON file containing project data."""

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return (
            "https://www3.dfc.gov/OPICProjects/Home/GetOPICActiveProjectList"
        )

    def get_projects(self) -> pd.DataFrame:
        """Downloads all development bank projects as JSON from DFC's website.

        NOTE: The endpoint does not have a valid SSL certificate, so
        verification is turned off for this request only.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch project data
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=requests.packages.urllib3.exceptions.InsecureRequestWarning,
            )
            r = self._data_request_client.post(
                url=self.download_url, verify=False
            )
            if not r.ok:
                raise RuntimeError(
                    "Error fetching DFC project records. "
                    f"The request failed with a "
                    f'"{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )

        # Parse projects into JSON
        try:
            df = pd.DataFrame.from_dict(r.json())
        except Exception as e:
            raise RuntimeError(
                f"Error parsing DFC projects into DataFrame. {e}"
            ) from None

        return df

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DFC project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Parse 'ProjectDetails' HTML column
            def parse_project_details(row: pd.Series) -> dict:
                """Extracts the project name, company, and URL from the row.

                Args:
                    row: The DataFrame row.

                Returns:
                    The new values and their corresponding keys.
                """
                details = row["ProjectDetails"]

                # Parse URL and project number from anchor tag
                url_match = re.search(r"<a href='(.*)' target", details)
                url = url_match.group(1) if url_match else ""
                number = (
                    "" if not url else url.replace(".pdf", "").split("/")[-1]
                )

                # Parse project affiliate and name in remaining HTML
                affiliate = re.search(r"<b>(.*)</b>", details)
                name = re.search(r"(?<=<br /><br />).*$", details)

                return {
                    "number": number,
                    "name": name.group(0) if name else "",
                    "affiliates": affiliate.group(1) if affiliate else "",
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
                """A generic function for grouping and concatenating values.

                Args:
                    group: The group.

                    col_name: The column for which to concatenate values.

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
            for grp_key, group in groups:
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
                        "url": grp_key,
                    }
                )

            # Replace NaN values with None
            df = df.replace({np.nan: None})

            # Replace None values with empty strings for string columns
            cols = [k for k, v in col_mapping.items() if v == "object"]
            df[cols] = df[cols].replace({None: ""})

            return pd.DataFrame(aggregated_projects)

        except Exception as e:
            raise RuntimeError(f"Error cleaning DFC projects. {e}") from None
