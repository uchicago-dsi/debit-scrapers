---
title: Insert Project Details
description:
---

Given a URL to one or more detailed project resources, request the URL, extract project data from the response content, map the projects to a common output schema, insert the projects into the database, and mark the current task a success.

## Example #1

The given URL corresponds to a project webpage (e.g., `bank.com/projects/coal-plant-in-sudan`). The workflow fetches the webpage, scrapes the HTML for project details, inserts the project in the database, and marks the task a success.

## Example #2

The given URL corresponds to an API query for a individual project view (e.g., `bank.com/api/projects/ak334hiJ7`). The workflow queries the API using the URL, parses the project data, inserts the project in the database, and marks the task a success.

## Example #3

The given URL corresponds to an API query for a page of detailed projects (e.g., `bank.com/api/projects?limit=10&offset=0`). The workflow queries the API using the URL, parses the project data, inserts the projects in the database, and marks the task a success.
