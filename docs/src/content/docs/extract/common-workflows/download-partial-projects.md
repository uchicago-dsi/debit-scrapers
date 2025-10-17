---
title: Download Partial Projects
description: A guide in my new Starlight docs site.
---

Request a project file (e.g., JSON, Excel, CSV, ZIP) hosted at a known URL, download its contents, and unzip it if necessary. Extract select fields from the project records and map them to the common output schema. In addition, generate URLs to detailed project resources to get remaining data. Persist the project data to the database, queue the next set of tasks, to process the URLs, and mark the current task a success.

## Example #1

The workflow downloads a file from `bank.com/projects/historical_projects.zip`, unzips its contents and then reads and processes the extracted Excel file for project data. Unfortunately, the data does not contain all of the desired project fields, but querying another API endpoint will provide the missing information. The workflow maps the extracted data to the common output schema, saves it to the database, constructs the API URLs, queues new tasks to process those URLs, and finally marks the current task a success.
