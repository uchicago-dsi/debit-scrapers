"""World Bank (WB)

Data is retrieved by downloading an Excel file of project records.

TODO: Thousands of older projects erroneously report commitments of $0.00.
The actual investment figures are embedded within attached documents
that have not yet been processed by the World Bank. Additional steps are
needed to fetch these documents and use LLMs to extract the investment amounts.
"""

# Standard library imports
import warnings
from io import BytesIO

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class WbDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads and parses an Excel file containing project data."""

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://search.worldbank.org/api/v3/projects/all.xlsx"

    @property
    def project_page_base_url(self) -> str:
        """The base URL for a World Bank project page."""
        return "https://projects.worldbank.org/en/projects-operations/project-detail/{}"

    def get_projects(self) -> pd.DataFrame:
        """Downloads an Excel file of projects from the WB's website.

        NOTE: The request may take a few minutes to complete
        due to the large file size.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Request data file
        r = self._data_request_client.get(
            self.download_url,
            use_random_user_agent=True,
            use_random_delay=True,
            timeout_in_seconds=600,
        )

        # Raise error if request failed
        if not r.ok:
            raise RuntimeError(
                "Error fetching Excel project data file "
                "from the World Bank. The request failed "
                f'with a "{r.status_code}-{r.reason}" '
                f'status code and the message "{r.text}'
                '".'
            ) from None

        # Load projects Excel worksheet into Pandas DataFrame
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                projects_df = pd.read_excel(
                    BytesIO(r.content),
                    skiprows=2,
                    engine="openpyxl",
                    sheet_name="World Bank Projects",
                )
        except Exception as e:
            raise RuntimeError(
                f"Error reading Excel project data "
                f"from the World Bank into Pandas DataFrame. {e}"
            ) from None

        # Load sectors Excel worksheet into Pandas DataFrame
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                sectors_df = pd.read_excel(
                    BytesIO(r.content),
                    skiprows=1,
                    engine="openpyxl",
                    sheet_name="Sectors",
                )
        except Exception as e:
            raise RuntimeError(
                f"Error reading Excel project sector data "
                f"from the World Bank into Pandas DataFrame. {e}"
            ) from None

        # Load financers Excel worksheet into Pandas DataFrame
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                financers_df = pd.read_excel(
                    BytesIO(r.content),
                    skiprows=1,
                    engine="openpyxl",
                    sheet_name="Financers",
                )
        except Exception as e:
            raise RuntimeError(
                f"Error reading Excel project financer data "
                f"from the World Bank into Pandas DataFrame. {e}"
            ) from None

        # Aggregate sectors and merge with projects as list field
        try:
            sectors_df["Sectors"] = sectors_df["Major Sector"].str.replace(
                "(Historic)", ""
            )
            agg_sectors_df = (
                sectors_df[["Project ID", "Sectors"]]
                .groupby("Project ID")
                .agg(lambda name: sorted(set(name)))
                .reset_index()
                .rename(columns={"Project ID": "id"})
            )
            projects_df = projects_df.merge(
                agg_sectors_df, how="left", on="id"
            )
        except Exception as e:
            raise RuntimeError(
                f"Error aggregating project sector data "
                f"and joining to projects DataFrame. {e}"
            ) from None

        # Aggregate financers and merge with projects as list field
        try:
            excluded = [  # noqa: F841
                "Local Communities",
                "Borrower/Recipient",
            ]
            financers_df = financers_df.query("Name not in @excluded")
            financers_df["Financers"] = financers_df["Name"].str.upper()
            agg_financers_df = (
                financers_df[["Project", "Financers"]]
                .groupby("Project")
                .agg(list)
                .reset_index()
                .rename(columns={"Project": "id"})
            )
            projects_df = projects_df.merge(
                agg_financers_df, how="left", on="id"
            )
        except Exception as e:
            raise RuntimeError(
                f"Error aggregating project financer data "
                f"and joining to projects DataFrame. {e}"
            ) from None

        return projects_df

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans World Bank project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Remove child projects and projects that were dropped
            df = df.query(
                "supplementprojectflg != 'Y' and status != 'Dropped'"
            )

            # Replace NaNs with None
            df = df.replace({np.nan: None})

            # Create bank column
            df["source"] = settings.WB_ABBREVIATION.upper()

            # Create number column
            df["number"] = df["id"]

            # Create name column
            df["name"] = df["project_name"]

            # Create financing type column
            def get_finance_type(row: pd.Series) -> str:
                """Determines a project's financing type(s).

                Args:
                    row: A row of data from the DataFrame.

                Returns:
                    The financing type(s) as a pipe-delimited string.
                """
                types = []
                if row["grantamt"]:
                    types.append("Grant")
                if row["curr_ibrd_commitment"] or row["idacommamt"]:
                    types.append("Loan")

                return "|".join(types) if types else "TBD"

            df["finance_types"] = df.apply(get_finance_type, axis=1)

            # Create approval date column
            df["date_approved"] = df["boardapprovaldate"].apply(
                lambda date: date[:10] if date else ""
            )

            # Create approval date column
            df["date_disclosed"] = df["public_disclosure_date"].apply(
                lambda date: date[:10] if date else ""
            )

            # Create effective date column
            df["date_effective"] = df["loan_effective_date"].apply(
                lambda date: date[:10] if date else ""
            )

            # Create closed date column
            df["date_actual_close"] = df["closingdate"].apply(
                lambda date: date[:10] if date else ""
            )

            # Create total commitment columns
            df["total_amount"] = df["total_amount_usd"] = df[
                "curr_total_commitment"
            ]
            df["total_amount_currency"] = df["total_amount"].apply(
                lambda val: "USD" if val is not None else ""
            )

            # Create countries column
            def correct_country_name(name: str) -> str:
                """Formats a country name.

                Rearranges a formal country name to remove
                its comma (e.g., "China, People's Republic
                of" becomes "People's Republic of China").
                At the time of writing, only one country
                is listed per project record, so combining
                different countries into one string is not
                a concern.

                Args:
                    name: The country name.

                Returns:
                    The formatted name.
                """
                if not name:
                    return ""

                num_formal_name_parts = 2
                name_parts = name.split(",")
                uses_formal_name = len(name_parts) == num_formal_name_parts
                if uses_formal_name:
                    return f"{name_parts[1].strip()} {name_parts[0]}"

                return name

            df["countries"] = df["countryshortname"].apply(
                correct_country_name
            )

            # Create sectors column
            df["sectors"] = df["Sectors"].apply(
                lambda lst: "|".join(lst) if lst else ""
            )

            # Create companies column
            def aggregate_affiliates(row: pd.Series) -> list[str]:
                """Aggregates a project's affiliated organizations.

                Args:
                    row: A row of data from the DataFrame.

                Returns:
                    The organization names as a pipe-delimited string.
                """
                affiliates = []
                if row["impagency"]:
                    affiliates.append(row["impagency"])
                if row["borrower"]:
                    affiliates.append(row["borrower"])
                if row["Financers"]:
                    affiliates.extend(row["Financers"])

                return "|".join(affiliates).upper() if affiliates else ""

            df["affiliates"] = df.apply(aggregate_affiliates, axis=1)

            # Create URL column
            df["url"] = df["number"].apply(
                lambda num: self.project_page_base_url.format(num)
            )

            # Subset records
            df = df[
                [
                    "affiliates",
                    "countries",
                    "date_approved",
                    "date_disclosed",
                    "date_effective",
                    "date_actual_close",
                    "finance_types",
                    "name",
                    "number",
                    "sectors",
                    "source",
                    "status",
                    "total_amount",
                    "total_amount_usd",
                    "total_amount_currency",
                    "url",
                ]
            ]

            # Replace None with empty strings in string data columns
            cols = [
                "affiliates",
                "countries",
                "date_approved",
                "date_disclosed",
                "date_effective",
                "date_actual_close",
                "finance_types",
                "name",
                "number",
                "sectors",
                "status",
                "total_amount_currency",
            ]
            df[cols] = df[cols].replace({None: ""})

            return df

        except Exception as e:
            raise RuntimeError(
                f"Error cleaning World Bank Project data. {e}"
            ) from None
