mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))

build:
	cd $(current_abs_path)
	docker-compose build

run:
	cd $(current_abs_path)
	docker-compose up