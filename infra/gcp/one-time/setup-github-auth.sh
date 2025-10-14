#!/bin/bash

# Google Cloud Workload Identity Federation Setup for GitHub Actions
#
# Creates a service account with the permissions necessary to deploy and
# manage resources using Pulumi. Permits GitHub Actions to impersonate
# that account as a Workload Identity within a larger pool and authenticate
# using OIDC tokens.
#
# Created by Claude Sonnet 4 and then edited, extended, and corrected.
#


# ------------------------------------------------------------------------
# SCRIPT CONFIGURATION
# ------------------------------------------------------------------------

# Configure exit on any error
set -e

# ------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NO_COLOR='\033[0m'

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NO_COLOR} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NO_COLOR} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NO_COLOR} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NO_COLOR}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to get user input with default value
get_input() {
    local prompt="$1"
    local default="$2"
    local result
    
    if [ -n "$default" ]; then
        read -p "$prompt [$default]: " result
        result="${result:-$default}"
    else
        read -p "$prompt: " result
    fi
    
    echo "$result"
}

# Function to confirm action
confirm() {
    local prompt="$1"
    local response
    
    read -p "$prompt (y/N): " response
    case "$response" in
        [yY][eE][sS]|[yY])
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# ------------------------------------------------------------------------
# GCLOUD CLI SETUP
# ------------------------------------------------------------------------

# Check prerequisites
print_header "Checking Prerequisites"

if ! command_exists gcloud; then
    print_error "gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    print_error "You are not authenticated with gcloud. Please run 'gcloud auth login' first."
    exit 1
fi

print_status "gcloud CLI is installed and authenticated"

# ------------------------------------------------------------------------
# USER PROMPTS
# ------------------------------------------------------------------------

# Get configuration from user
print_header "Configuration"

PROJECT_ID=$(get_input "Enter your Google Cloud Project ID" "$(gcloud config get-value project 2>/dev/null)")
GITHUB_USERNAME=$(get_input "Enter the GitHub username or organization housing the repository" "uchicago-dsi")
REPO_NAME=$(get_input "Enter your GitHub repository name" "debit-scrapers")

# Optional customization
POOL_NAME=$(get_input "Workload Identity Pool name" "github-actions-pool")
PROVIDER_NAME=$(get_input "OIDC Provider name" "github-actions-provider")
SERVICE_ACCOUNT_NAME=$(get_input "Service Account name" "github-actions-sa")

# Derive full names
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

print_status "Configuration:"
print_status "  Project ID: $PROJECT_ID"
print_status "  GitHub Repo: $GITHUB_USERNAME/$REPO_NAME"
print_status "  Pool Name: $POOL_NAME"
print_status "  Provider Name: $PROVIDER_NAME"
print_status "  Service Account: $SERVICE_ACCOUNT_EMAIL"

if ! confirm "Continue with this configuration?"; then
    print_status "Setup cancelled."
    exit 0
fi

# ------------------------------------------------------------------------
# PROJECT SELECTION
# ------------------------------------------------------------------------

# Set the project
print_header "Setting up Google Cloud Project"
gcloud config set project "$PROJECT_ID"

# Get project number
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
print_status "Project Number: $PROJECT_NUMBER"

# ------------------------------------------------------------------------
# API ACTIVATION
# ------------------------------------------------------------------------

# Enable required APIs
print_header "Enabling required APIs..."
print_status "Enabling IAM Credentials API..."
gcloud services enable iamcredentials.googleapis.com

print_status "Enabling Security Token Service API..."
gcloud services enable sts.googleapis.com

# ------------------------------------------------------------------------
# WORKLOAD IDENTITY POOL CONFIGUATION
# ------------------------------------------------------------------------

# Create Workload Identity Pool
print_header "Creating Workload Identity Pool"

if gcloud iam workload-identity-pools describe "$POOL_NAME" --location="global" --project="$PROJECT_ID" >/dev/null 2>&1; then
    print_warning "Workload Identity Pool '$POOL_NAME' already exists. Skipping creation."
else
    print_status "Creating workload identity pool: $POOL_NAME"
    gcloud iam workload-identity-pools create "$POOL_NAME" \
        --project="$PROJECT_ID" \
        --location="global" \
        --display-name="GitHub Actions Pool"
    print_status "Workload Identity Pool created successfully"
fi

# Create OIDC Provider
print_header "Creating OIDC Provider"

if gcloud iam workload-identity-pools providers describe "$PROVIDER_NAME" \
    --workload-identity-pool="$POOL_NAME" \
    --location="global" \
    --project="$PROJECT_ID" >/dev/null 2>&1; then
    print_warning "OIDC Provider '$PROVIDER_NAME' already exists. Skipping creation."
else
    print_status "Creating OIDC provider: $PROVIDER_NAME"
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_NAME" \
        --project="$PROJECT_ID" \
        --location="global" \
        --workload-identity-pool="$POOL_NAME" \
        --display-name="GitHub Actions Provider" \
        --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
        --attribute-condition="assertion.repository_owner == '$GITHUB_USERNAME'" \
        --issuer-uri="https://token.actions.githubusercontent.com"
    print_status "OIDC Provider created successfully"
fi

# ------------------------------------------------------------------------
# SERVICE ACCOUNT CONFIGURATION
# ------------------------------------------------------------------------

# Create Service Account
print_header "Creating Service Account"

if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$PROJECT_ID" >/dev/null 2>&1; then
    print_warning "Service Account '$SERVICE_ACCOUNT_EMAIL' already exists. Skipping creation."
else
    print_status "Creating service account: $SERVICE_ACCOUNT_NAME"
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --project="$PROJECT_ID" \
        --display-name="GitHub Actions Service Account"
    print_status "Service Account created successfully"
fi

# Add IAM roles to service account to enable deployment with Pulumi
print_header "Configuring IAM Roles"

declare -A ROLES=(
    ["roles/artifactregistry.admin"]="Artifact Registry Admin"
    ["roles/run.admin"]="Cloud Run Admin"
    ["roles/cloudscheduler.admin"]="Cloud Scheduler Admin"
    ["roles/cloudsql.admin"]="Cloud SQL Admin"
    ["roles/cloudtasks.admin"]="Cloud Tasks Admin"
    ["roles/cloudtasks.queueAdmin"]="Cloud Tasks Queue Admin"
    ["roles/compute.admin"]="Compute Admin"
    ["roles/eventarc.admin"]="Eventarc Admin"
    ["roles/resourcemanager.projectIamAdmin"]="Project IAM Admin"
    ["roles/secretmanager.admin"]="Secret Manager Admin"
    ["roles/iam.serviceAccountAdmin"]="Service Account Admin"
    ["roles/serviceusage.serviceUsageAdmin"]="Service Usage Admin"
    ["roles/storage.admin"]="Storage Admin"
    ["roles/workflows.admin"]="Workflows Admin"
)

for ROLE in "${!ROLES[@]}"; do
  print_status "Adding ${ROLES[$ROLE]} role to service account..."
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
      --role="$ROLE" \
      --quiet
done

# ------------------------------------------------------------------------
# WORKLOAD IDENTITY CONFIGURATION
# ------------------------------------------------------------------------

# Configure Workload Identity
print_header "Configuring Workload Identity"

# Allow GitHub repository to impersonate service account
print_status "Allowing GitHub repository to impersonate service account..."
gcloud iam service-accounts add-iam-policy-binding \
    "$SERVICE_ACCOUNT_EMAIL" \
    --project="$PROJECT_ID" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_NAME/attribute.repository/$GITHUB_USERNAME/$REPO_NAME"

# Get the Workload Identity Provider resource name
WORKLOAD_IDENTITY_PROVIDER=$(gcloud iam workload-identity-pools providers describe "$PROVIDER_NAME" \
    --project="$PROJECT_ID" \
    --location="global" \
    --workload-identity-pool="$POOL_NAME" \
    --format="value(name)")

# ------------------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------------------

# Print configuration for GitHub Actions
print_header "Setup Complete!"

print_status "Your Workload Identity Federation is now configured."
echo
print_header "GitHub Actions Configuration"
echo "Add these as repository variables (Settings > Secrets and variables > Actions > Variables):"
echo
echo "GCP_WORKLOAD_IDENTITY_PROVIDER: $WORKLOAD_IDENTITY_PROVIDER"
echo "GCP_GITHUB_SERVICE_ACCOUNT_EMAIL: $SERVICE_ACCOUNT_EMAIL"
echo "GCP_PROJECT_ID: $PROJECT_ID"
echo
print_header "Sample GitHub Actions Workflow"
cat << EOF
name: Deploy with OIDC

on:
  workflow_dispatch:
  push:
    branches: [main]

permissions:
  id-token: write
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Authenticate to Google Cloud
      uses: google-github-actions/auth@v2
      with:
        workload_identity_provider: \${{ vars.WORKLOAD_IDENTITY_PROVIDER }}
        service_account: \${{ vars.SERVICE_ACCOUNT_EMAIL }}
    
    - name: Set up Cloud SDK
      uses: google-github-actions/setup-gcloud@v2
    
    - name: Test authentication
      run: gcloud auth list
EOF
echo
print_status "Setup completed successfully! 🎉"
print_status "Remember to add the repository variables to your GitHub repository!"
print_status "Script completed."