---
sidebar_position: 4
---

# Crawl & Scrape Search Results

Given a URL to a less-detailed list view of projects, request the URL, construct links to individual project resources, _and_ extract incomplete project records using the request body content. Insert the incomplete project records into the database, queue new tasks for processing the remaining project data at those links, and mark the current task a success.

Note: This workflow is necessary when neither a project summary view nor detailed project view hold complete information about a project on their own but do when combined. It is also useful when the summary view has a data field that is much easier to extract compared to the same field in a detailed view.

## Example #1

The given URL corresponds to a project search result webpage (e.g., `bank.com/projects?page=0&limit=10`). The workflow fetches the webpage and scrapes the HTML for links to individual project webpages (e.g., `bank.com/projects/coal-plant-in-sudan`) and partial project data (e.g., approval date, countries), Finally, it queues new tasks for processing those links and marks the current task a success.

## Example #2

The given URL corresponds to an API query for a paginated list view of projects (e.g., `bank.com/api/projects?limit=10&offset=0`). The workflow queries the API using the URL, parses the projects, and then generates URLs to request more detailed, individual project representations from the API (e.g., `bank.com/api/projects/ak334hiJ7`). It then queues new tasks and marks the current task a success.
