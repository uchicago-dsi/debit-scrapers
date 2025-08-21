mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))
project_name := "debit-scrapers"
container_dir := "src"

run-api:
	cd $(current_abs_path)
	docker-compose up

build-scrapers:
	cd $(current_abs_path)
	docker build -t $(project_name) $(current_abs_path)/pipeline

run-scrapers-bash:
	cd $(current_abs_path)
	docker run \
		--name $(project_name) \
		-v $(current_abs_path)pipeline:/$(container_dir) \
		-it \
		--env-file ./pipeline/.env.dev \
		--rm $(project_name) bash

build-scraper-webserver:
	cd $(current_abs_path)
	docker compose --profile pipeline build

run-scraper-webserver:
	cd $(current_abs_path)
	docker compose --profile pipeline up