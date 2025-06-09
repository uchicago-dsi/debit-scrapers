#!/bin/bash
#
# Starts the Django server. Optionally
# creates and applies new data migrations
# and/or populates tables with initial data
# based on command line arguments.
#
###

# Configure script to exit when any command fails
set -e

# Monitor last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG

# Log error message upon script exit
trap '[ $? -eq 1 ] && echo "Database setup failed."' EXIT

# Parse command line arguments
migrate=false
load_datasets=false
use_uwsgi_server=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --migrate) migrate=true; shift ;;
        --load) load_datasets=true; shift ;;
        --use_uwsgi_server) use_uwsgi_server=true; shift ;;
        *) echo "Unknown command line parameter received: $1"; exit 1 ;;
    esac
done

# Perform data migrations if indicated
if $migrate ; then
    echo "Creating database migrations from Django models."
    yes | ./manage.py makemigrations

    echo "Applying migrations to database."
    yes | ./manage.py migrate
fi

# Load initial datasets if indicated
if $load_datasets ; then

    dataset_dir="$PWD/apps"

    echo "Loading banks into database."
    ./manage.py loaddata "$dataset_dir/banks/fixtures/bank.json"

    echo "Loading IAMs into database."
    ./manage.py loaddata "$dataset_dir/banks/fixtures/iam.json"

    echo "Loading bank IAMs into database."
    ./manage.py loaddata "$dataset_dir/banks/fixtures/bank_iam.json"

    echo "Loading countries into database."
    ./manage.py loaddata "$dataset_dir/countries/fixtures/country.json"

    echo "Loading sectors into database."
    ./manage.py loaddata "$dataset_dir/sectors/fixtures/sector.json"
fi

# Run server
if $use_uwsgi_server ; then
    echo "Running UWSGI server."
    uwsgi --http ":8080" \
        --chdir "/usr/src/api" \
        --module "config.wsgi:application" \
        --uid "1000" \
        --gid "2000" \
        --http-timeout "1000"
else
    echo "Running default development server."
    ./manage.py runserver 0.0.0.0:8080
fi
