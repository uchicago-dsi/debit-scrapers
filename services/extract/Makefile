# Define absolute path to Makefile parent directory
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))

# Define path to Dockerfile
dockerfile_path := $(current_abs_path)Dockerfile.heavy

# Define source directories for mounting
project_src_dir := $(current_abs_path)src
container_src_dir := "app/src"

# Define additional additional Docker variables
project_name := "debit-scrapers"
env_path := $(current_abs_path).env.dev

# Define commands
build-scrapers:
	cd $(current_abs_path)
	docker build -t $(project_name) -f $(dockerfile_path) .

run-scrapers-bash: build-scrapers
	docker run --name $(project_name) \
		-v $(project_src_dir):/$(container_src_dir) \
		-it \
		--env-file $(env_path) \
		--rm $(project_name) \
		bash

run-scrapers-network:
	cd $(current_abs_path)
	docker compose up --build

tear-down-scrapers-network:
	cd $(current_abs_path)
	docker compose down
	sudo chown -R $(USER) pgdata
	rm -R pgdata