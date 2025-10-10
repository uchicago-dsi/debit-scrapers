"""U.S. International Development Finance Corporation (DFC)

The organization is formally known as the Overseas Private Investment
Corporation (OPIC). Data is retrieved through a direct download.
"""

# Standard library imports
import io

# Third-party imports
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class DfcDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads and parses a JSON file containing project data."""

    @property
    def download_url(self) -> str:
        """The URL to a resource containing all project records."""
        return "https://www3.dfc.gov/OPICProjects/Data/ActiveProjects.xlsx"

    def get_projects(self) -> pd.DataFrame:
        """Downloads all development bank projects as XLSX from DFC's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch project data
        r = self._data_request_client.get(
            url=self.download_url, use_random_user_agent=True
        )
        if not r.ok:
            raise RuntimeError(
                "Error fetching DFC project records. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse projects into DataFrame
        try:
            df = pd.read_excel(io.BytesIO(r.content))
        except Exception as e:
            raise RuntimeError(
                f"Error parsing DFC projects into DataFrame. {e}"
            ) from None

        return df

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DFC project records to conform to an expected schema.

        NOTE: Unique DFC projects can point to the same project page URL
        if they are related, which breaks current database constraints.
        To work around this issue, a faux anchor link with the project
        number is appended to the URL whenever more than one reference to
        that URL exists (e.g., "https://www.dfc.gov/sites/default/files/media/documents/9000115501_0.pdf#9000115550").

        References:
        - https://www.dfc.gov/what-we-do/active-projects
        - https://www.dfc.gov/our-impact/transaction-data

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Drop records w/o project page URLs (public information sheets)
            df = df.query("`Project Profile URL` == `Project Profile URL`")

            # Count frequency of project page URLs
            url_frequencies_df = (
                df[["Project Profile URL"]]
                .groupby("Project Profile URL")
                .size()
                .reset_index()
                .rename(columns={0: "URL Frequency"})
            )

            # Merge frequencies with existing data
            df = df.merge(
                url_frequencies_df, on="Project Profile URL", how="left"
            )

            # Create output columns
            df["countries"] = df["Country"]
            df["fiscal_year_effective"] = df["Fiscal_Year"].astype(int)
            df["finance_types"] = df["Project Type"]
            df["name"] = df["Project Name"]
            df["number"] = df["Project Number"]
            df["sectors"] = df["NAICS Sector"]
            df["source"] = settings.DFC_ABBREVIATION.upper()
            df["total_amount"] = df["total_amount_usd"] = df["Committed"]
            df["total_amount_currency"] = "USD"
            df["url"] = df.apply(
                lambda row: row["Project Profile URL"]
                + (
                    ""
                    if row["URL Frequency"] == 1
                    else f"#{row['URL Frequency']}"
                ),
                axis=1,
            )

            # Finalize columns
            df = df[
                [
                    "countries",
                    "fiscal_year_effective",
                    "finance_types",
                    "name",
                    "number",
                    "sectors",
                    "source",
                    "total_amount",
                    "total_amount_currency",
                    "url",
                ]
            ]

            return df

        except Exception as e:
            raise RuntimeError(f"Failed to clean DFC projects. {e}") from None
