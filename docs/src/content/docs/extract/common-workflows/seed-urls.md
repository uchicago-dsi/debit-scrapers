---
title: Seed URLs
description: A guide in my new Starlight docs site.
---

Request a known starting point for web crawling and then use data from the response to generate the next set of URLs to crawl. Queue those tasks and mark the current task a success.

**_Example #1_**. A starting point could be a bank's project search page (e.g., `bank.com/projects`), in which projects are displayed as a paginated list of cards. After requesting the website, a workflow would parse the HTML to find the number of the last page and then generate URLs to all search result pages (e.g., `bank.com/projects?page=0`, `bank.com/projects?page=1`, etc.). Finally, the workflow would queue new tasks for processing those URLs and mark the current task a success.

**_Example #2_**. Another starting point could be an API query (e.g., `bank.com/api/projects?limit=0&offset=0`) whose response body contains a page of projects and metadata about the total number of results. After querying the API, a workflow would use the metadata to generate URLs to all search result resources (e.g., `bank.com/api/projects?limit=10&offset=0`, `bank.com/api/projects?limit=10&offset=10`, etc.). Finally, the workflow would queue new tasks for processing those URLs and mark the current task a success.
