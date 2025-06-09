#
# deploy.sh
#
# Builds a Docker image of the project directory
# and then tags and pushes that image to the
# Google Artifact Registry.
#
# References
# - https://cloud.google.com/artifact-registry/docs/docker/pushing-and-pulling
#
###

# Define parameters
LOCATION=us-central1
PROJECT_ID=southern-field-305921
REPOSITORY=debit-docker-repo
IMAGE_NAME=debit-api
REGISTRY_IMAGE_NAME="$LOCATION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME"

# Build Docker image locally and then push to registry
docker build -t $IMAGE_NAME .
docker tag $IMAGE_NAME $REGISTRY_IMAGE_NAME
docker push $REGISTRY_IMAGE_NAME
