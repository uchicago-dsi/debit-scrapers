---
title: Download Complete Projects
description:
---

Request a project file (e.g., JSON, Excel, CSV, ZIP) hosted at a known URL, download its contents, unzip it if necessary, and map the project records to the common output schema. Persist the project data to the database and mark the current task a success.

## Example #1

The workflow downloads a file from `bank.com/projects/historical_projects.zip`, unzips its contents and then reads and processes the extracted Excel file for project data. Finally, it maps the data to the common output schema, saves it to the database, and marks the current task a success.
