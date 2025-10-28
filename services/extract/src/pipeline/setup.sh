#!/bin/bash

# Log script start
echo "Starting setup script."

# Configure script to exit when any command fails
set -e

# Monitor last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG

# Log error message upon script exit
trap 'status=$?; if [ $status -ne 0 ]; then echo "An unexpected error occurred (exit $status)."; fi' EXIT

# Parse command line arguments
migrate=false
extract_data=false
force_restart=false
run_server=false
development=false
date=""
truncate=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --migrate) migrate=true; shift ;;
        --extract-data) extract_data=true; shift ;;
        --force-restart) force_restart=true; shift ;;
        --run-server) run_server=true; shift ;;
        --development) development=true; shift ;;
        --date)
            if [[ -n "$2" && "$2" != --* ]]; then
                date="$2"
                shift 2
            else
                echo "Error: --date requires a value"
                exit 1
            fi
            ;;
        --truncate) truncate=true; shift ;;
        *) echo "Unknown command line parameter received: $1"; exit 1 ;;
    esac
done

# Perform model migrations if indicated 
# (WARNING: Defaults to "yes" for all questions)
if $migrate ; then
    echo "Creating database migrations from Django models."
    yes | uv run python manage.py makemigrations

    echo "Applying migrations to database."
    yes | uv run python manage.py migrate

    echo "Database setup completed successfully."
fi

# Extract latest development project data if indicated
if $extract_data ; then
    echo "Orchestrating data extraction."
    orchestrator_cmd="uv run python manage.py orchestrate"
    if $force_restart ; then
        orchestrator_cmd="$orchestrator_cmd --force-restart"
    fi
    if [[ -n "$date" ]]; then
        orchestrator_cmd="$orchestrator_cmd --date \"$date\""
    fi
    eval $orchestrator_cmd
fi

# Truncate database if indicated
if $truncate ; then
    echo "Truncating database."
    uv run python manage.py truncate
fi

# Exit if not running server
if ! $run_server ; then
    exit 0
fi

# Otherwise, run server corresponding to environment
if $development ; then
    echo "Running default development server."
    uv run python manage.py runserver 0.0.0.0:8000 --verbosity 2
else 
    echo "Running production server."
    uv run gunicorn --config python:config.gunicorn config.wsgi
fi