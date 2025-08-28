mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))

project_name := "debit-scrapers"
container_dir := "src"
dockerfile_path := $(current_abs_path)src/Dockerfile.heavy
env_path := $(current_abs_path)src/.env.dev
project_path := $(current_abs_path)src

build-scrapers:
	cd $(current_abs_path)
	docker build -t $(project_name) -f $(dockerfile_path) $(project_path)

run-scrapers-bash:
	cd $(current_abs_path)
	docker run --name $(project_name) \
		-v $(project_path):/$(container_dir) \
		-it \
		--env-file $(env_path) \
		--rm $(project_name) \
		bash