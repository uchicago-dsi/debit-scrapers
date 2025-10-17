---
title: Crawl Search Results
description: A guide in my new Starlight docs site.
---

Given a URL to a less-detailed list view of projects, request the URL and then construct links to individual project resources using the request body content. Finally, queue new tasks for processing those links and mark the current task a success.

**_Example #1_**. The given URL corresponds to a project search result webpage (e.g., `bank.com/projects?page=0&limit=10`). The workflow fetches the webpage, scrapes the HTML for links to individual project webpages (e.g., `bank.com/projects/coal-plant-in-sudan`), and queues new tasks for processing those links. It then marks the current task a success.

**_Example #2_**. The given URL corresponds to an API query for a paginated list view of projects (e.g., `bank.com/api/projects?limit=10&offset=0`). The workflow queries the API using the URL, parses the projects, and then generates URLs to request more detailed, individual project representations from the API (e.g., `bank.com/api/projects/ak334hiJ7`). It then queues new tasks and marks the current task a success.
