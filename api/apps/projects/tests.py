"""Tests for the projects application.
"""

import json
from apps.banks.factories import BankFactory
from django.forms.models import model_to_dict
from django.urls import reverse
from io import BytesIO
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase
from .factories import ProjectFactory
from apps.countries.factories import CountryFactory
from apps.projects.factories import ProjectSectorFactory, ProjectCountryFactory
from apps.sectors.factories import SectorFactory
from types import SimpleNamespace

class ProjectTests(APITestCase):
    """Test cases for projects.
    """

    def test_get_project(self):
        """Asserts that a project can be fetched by id.
        """
        # Create project in database
        project = ProjectFactory.create()

        # Fetch same project using API
        url = reverse("project-detail", args=[project.id])
        response = self.client.get(url, format="json")
        data = response.json()

        # Assert that the project was successfully returned
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(data.get("id"), project.id)
    
    # def test_search_projects_by_name(self):
    #     """Asserts that projects can be fetched by name.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(count=10, name="Target Project"))
    #     ProjectFactory.create_batch(target.count, name=target.name)

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(count=5, name="Other Project"))
    #     ProjectFactory.create_batch(other.count, name=other.name)
        
    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"name": target.name})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_bank(self):
    #     """Asserts that projects can be fetched by bank.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         bank=BankFactory.create(abbrev_name="Bank A")
    #     ))
    #     ProjectFactory.create_batch(target.count, bank_id=target.bank)

    #     # Create extraneous projects in databasee
    #     other = SimpleNamespace(**dict(
    #         count=8, 
    #         bank=BankFactory.create(abbrev_name="Bank B")
    #     ))
    #     ProjectFactory.create_batch(other.count, bank_id=other.bank)

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"bank": target.bank.abbrev_name})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_country(self):
    #     """Asserts that projects can be fetched by country name.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         country="Tanzania"
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         country_list_stnd=f"{target.country}, United States"
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(count=5))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         country_list_stnd="Russia, Zimbabwe"
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"countries": target.country})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_sector(self):
    #     """Asserts that projects can be fetched by sector name.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         sector="Environment"
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         sector_list_stnd=f"{target.sector}, Manufacturing, Other"
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(count=5))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         sector_list_stnd="Agribusiness, Energy, Forestry"
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"sectors": target.sector})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_company(self):
    #     """Asserts that projects can be fetched by company name.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         company="Company A"
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         companies=target.company
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(
    #         count=5,
    #         company="Company B"
    #     ))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         companies=other.company
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"companies": target.company})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_status(self):
    #     """Asserts that projects can be fetched by status.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         status="In Progress"
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         status=target.status
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(
    #         count=5,
    #         status="Completed"
    #     ))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         status=other.status
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"status": target.status})
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_by_keyword(self):
    #     """Asserts that projects can be fetched by a multi-field keyword.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         schema={
    #             "status": "Pending",
    #             "country_list_stnd": "Bosnia"
    #         }
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         **target.schema
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(
    #         count=5,
    #         schema={
    #             "status": "Completed",
    #             "country_list_stnd": "United Kingdom"
    #         }
    #     ))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         **other.schema
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={
    #         "search": target.schema["status"]
    #     })
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_search_projects_options(self):
    #     """Asserts that projects can be fetched with 
    #     sorting, limits, and offsets applied.
    #     """
    #     # Create projects with alphabetized names
    #     projects = []
    #     for i in range(10):
    #         projects.append(ProjectFactory.create(name=f"Project {i + 1}"))

    #     # Set search options
    #     sort_col = "name"
    #     sort_desc = 1
    #     limit = 2
    #     offset = 4

    #     # Determine target projects
    #     projects.sort(
    #         key=lambda p: getattr(p, sort_col), 
    #         reverse=bool(sort_desc)
    #     )
    #     targets = projects[offset: offset + limit]

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={
    #         "limit": limit,
    #         "offset": offset,
    #         "sortCol": sort_col,
    #         "sortDesc": sort_desc 
    #     })
    #     data = response.json()["projects"]

    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), len(targets))
    #     self.assertEqual(
    #         first=set(d["id"] for d in data),
    #         second=set(t.id for t in targets)
    #     )

    # @parameterized.expand([(5), (50), (500)])
    # def test_search_projects_download_size(self, size: int):
    #     """Asserts that projects can be downloaded successfully
    #     given different batch sizes.
    #     """
    #     # Create projects
    #     projects = ProjectFactory.create_batch(size)

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={"download": 1})

    #     # Stream results to in-memory file and then parse to JSON
    #     mem_file = BytesIO()
    #     for r in response.streaming_content:
    #         mem_file.write(r)
    #     mem_file.seek(0)
    #     data = json.load(mem_file)
            
    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), len(projects))

    # def test_search_projects_download_query(self):
    #     """Asserts that batches of projects can be downloaded
    #     successfully following a search query.
    #     """
    #     # Create target projects in database
    #     target = SimpleNamespace(**dict(
    #         count=3, 
    #         status="In Progress"
    #     ))
    #     ProjectFactory.create_batch(
    #         target.count,
    #         status=target.status
    #     )

    #     # Create extraneous projects in database
    #     other = SimpleNamespace(**dict(
    #         count=5,
    #         status="Completed"
    #     ))
    #     ProjectFactory.create_batch(
    #         other.count,
    #         status=other.status
    #     )

    #     # Search for target projects using API
    #     url = reverse("project-search")
    #     response = self.client.post(url, data={
    #         "download": 1,
    #         "status": target.status
    #     })

    #     # Stream results to in-memory file and then parse to JSON
    #     mem_file = BytesIO()
    #     for r in response.streaming_content:
    #         mem_file.write(r)
    #     mem_file.seek(0)
    #     data = json.load(mem_file)
            
    #     # Assert target projects successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), target.count)

    # def test_insert_projects(self):
    #     """Asserts that projects can be bulk inserted into the database.
    #     """
    #     # Build projects
    #     bank = BankFactory.create()
    #     projects = ProjectFactory.build_batch(100, bank_id=bank)
    #     proj_json = [model_to_dict(p) for p in projects]

    #     # Create projects in database using API
    #     url = reverse("project-list")
    #     response = self.client.post(
    #         path=url,
    #         data={"records": proj_json},
    #         format="json"
    #     )
    #     data = response.json()

    #     # Assert that the projects were successfully created
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), len(projects))

    # def test_upsert_projects(self):
    #     """Asserts that projects can be bulk upserted into the database.
    #     """
    #     # Create project in database
    #     original_fields = {
    #         "name": "Project Original",
    #         "status": "In Progress",
    #         "year": 2022,
    #         "month": 5,
    #         "day": 2,
    #         "loan_amount": 100_000,
    #         "loan_amount_currency": "USD",
    #         "loan_amount_in_usd": 100_000,
    #         "sector_list_raw": "energy",
    #         "sector_list_stnd": "Energy",
    #         "companies": "Test Company",
    #         "country_list_raw": "kenya",
    #         "country_list_stnd": "Kenya",
    #         "url": "https://test.com"
    #     }
    #     project = ProjectFactory.create(**original_fields)

    #     # Create project updates
    #     updated_fields = {
    #         "name": "Project Updated",
    #         "status": "Completed",
    #         "year": 2023,
    #         "month": 2,
    #         "day": 1,
    #         "loan_amount": 300_000,
    #         "loan_amount_currency": "EUR",
    #         "loan_amount_in_usd": 250_000,
    #         "sector_list_raw": "environment",
    #         "sector_list_stnd": "Environment",
    #         "companies": "Test Company 2",
    #         "country_list_raw": "uganda",
    #         "country_list_stnd": "Uganda"
    #     }
    #     for k, v in updated_fields.items():
    #         setattr(project, k, v)

    #     # Fetch same project using API
    #     url = reverse("project-list")
    #     response = self.client.post(
    #         path=url,
    #         data={"records": [model_to_dict(project)]},
    #         format="json"
    #     )
    #     data = response.json()

    #     # Assert that the project was successfully returned
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), 1)
    #     for k, v in updated_fields.items():
    #         self.assertEqual(data[0][k], v)

    # def test_insert_project_countries(self):
    #     """Asserts that project-country associations can be 
    #     bulk inserted into the database.
    #     """
    #     # Build projects
    #     country = CountryFactory.create()
    #     projects = ProjectFactory.create_batch(100)
    #     proj_country_json = [
    #         model_to_dict(ProjectCountryFactory.build(
    #             country_id=country,
    #             project_id=project
    #         ))
    #         for project in projects
    #     ]

    #     # Create projects in database using API
    #     url = reverse("project-country-list")
    #     response = self.client.post(
    #         path=url,
    #         data={"records": proj_country_json},
    #         format="json"
    #     )
    #     data = response.json()

    #     # Assert that the project-countries were successfully created
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(len(data), len(proj_country_json))

    def test_insert_project_sectors(self):
        """Asserts that project-sector associations can be 
        bulk inserted into the database.
        """
        # Create sector record in database
        sector = SectorFactory.create()
        projects = ProjectFactory.create_batch(100)

        # Build project records for insert
        records = []
        for project in projects:
            proj_sector = model_to_dict(ProjectSectorFactory.build(
                sector=sector,
                project=project
            ))
            proj_sector.pop("id")
            records.append(proj_sector)

        # Create projects in database using API
        url = reverse("project-sector-list")
        response = self.client.post(
            path=url,
            data=records,
            format="json"
        )
        data = response.json()

        # Assert that the project-sectors were successfully created
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(data), len(records))
