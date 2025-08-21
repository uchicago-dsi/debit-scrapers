---
sidebar_position: 6
---

# Update Project Details

Given a URL to one or more detailed project resources, request the URL, extract partial project data from the response content, map the projects to a common output schema, update the projects' pre-existing representations in the database, and mark the current task a success.

## Example #1

The given URL corresponds to a project webpage (e.g., `bank.com/projects/coal-plant-in-sudan`). The workflow fetches the webpage, scrapes the HTML for select project fields, updates the project in the database, and marks the task a success.

## Example #2

The given URL corresponds to an API query for a individual project view (e.g., `bank.com/api/projects/ak334hiJ7`). The workflow queries the API using the URL, parses the project data to extract select fields, updates the project in the database, and marks the task a success.
