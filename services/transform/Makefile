# Define absolute path to Makefile parent directory
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))

# Define input directories for mounting
input_dir := $(current_abs_path)input
container_input_dir := "app/input"

# Define output directories for mounting
output_dir := $(current_abs_path)output
container_output_dir := "app/output"

# Define additional Docker variables
project_name := $(current_dir)
env_path := $(current_abs_path).env.dev
input_file := extracted.tsv.gz

# Define commands
build-clean:
	cd $(current_abs_path)
	docker build -t $(project_name) .

run-clean: build-clean
	cd $(current_abs_path)
	docker run --name $(project_name) \
		-v $(input_dir):/$(container_input_dir) \
		-v $(output_dir):/$(container_output_dir) \
		--env-file $(env_path) \
		--rm $(project_name) ${input_file}