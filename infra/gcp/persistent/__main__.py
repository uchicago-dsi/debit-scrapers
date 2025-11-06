"""A script to automate infrastructure deployment using Pulumi."""

# Third-party imports
import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
import pulumi_std as std

# Application imports
from constants import (
    CLOUDFLARE_R2_ACCESS_KEY_ID,
    CLOUDFLARE_R2_BUCKET_URL,
    CLOUDFLARE_R2_ENDPOINT_URL,
    CLOUDFLARE_R2_SECRET_ACCESS_KEY,
    DJANGO_ALLOWED_HOST,
    DJANGO_API_PATH_DATA_EXTRACTION,
    DJANGO_PORT,
    DJANGO_SECRET_KEY,
    DJANGO_SETTINGS_MODULE,
    ENV,
    EXTRACT_DIR,
    EXTRACTION_PIPELINE_MAX_RETRIES,
    EXTRACTION_PIPELINE_MAX_WAIT,
    EXTRACTION_PIPELINE_POLLING_INTERVAL,
    EXTRACTION_PIPELINE_SCHEDULE,
    GEMINI_API_KEY,
    IS_TEST,
    MAPPING_DIR,
    OUTPUT_FILE_MAX_AGE,
    OUTPUT_FILE_NAME,
    OUTPUT_FILE_TOTAL_MAX_ATTEMPTS,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    PROJECT_ID,
    PROJECT_REGION,
    TRANSFORM_DIR,
    QUEUE_CONFIG,
)

# ------------------------------------------------------------------------
# Provider
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/provider/
# ------------------------------------------------------------------------

# region

gcp_provider = gcp.Provider(
    f"debit-{ENV}-gcp-provider",
    default_labels={
        "project": "debit",
        "organization": "idi",
        "environment": ENV,
        "managed-by": "uchicago-dsi",
        "automation": "pulumi",
    },
)

# endregion

# ------------------------------------------------------------------------
# Projects
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/projects/
# ------------------------------------------------------------------------

# region

required_services = [
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "sqladmin.googleapis.com",
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "cloudtasks.googleapis.com",
    "workflows.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "pubsub.googleapis.com",
    "eventarc.googleapis.com",
]

enabled_services = [
    gcp.projects.Service(
        f"debit-{ENV}-enable-{svc.split('.')[0]}",
        service=svc,
        project=PROJECT_ID,
        disable_on_destroy=False,
    )
    for svc in required_services
]

# endregion

# ------------------------------------------------------------------------
# Secret Manager
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/secretmanager/
# ------------------------------------------------------------------------

# region

# Create Django secret key
django_secret = gcp.secretmanager.Secret(
    f"debit-{ENV}-secret-django-secret",
    secret_id=f"debit-{ENV}-secret-django-secret",
    replication=gcp.secretmanager.SecretReplicationArgs(
        user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
            replicas=[
                gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                    location=PROJECT_REGION
                ),
            ]
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
django_secret_version = gcp.secretmanager.SecretVersion(
    f"debit-{ENV}-secret-version-django-secret",
    secret=django_secret.id,
    secret_data=DJANGO_SECRET_KEY,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("django_secret_key", django_secret.name)

# Create Postgres password
postgres_password = gcp.secretmanager.Secret(
    f"debit-{ENV}-secret-postgres-password",
    secret_id=f"debit-{ENV}-secret-postgres-password",
    replication=gcp.secretmanager.SecretReplicationArgs(
        user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
            replicas=[
                gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                    location=PROJECT_REGION
                ),
            ]
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
postgres_password_version = gcp.secretmanager.SecretVersion(
    f"debit-{ENV}-secret-version-postgres-password",
    secret=postgres_password.id,
    secret_data=POSTGRES_PASSWORD,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("postgres_password", postgres_password.name)

# Create Gemini API key
gemini_api_key = gcp.secretmanager.Secret(
    f"debit-{ENV}-secret-gemini-api-key",
    secret_id=f"debit-{ENV}-secret-gemini-api-key",
    replication=gcp.secretmanager.SecretReplicationArgs(
        user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
            replicas=[
                gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                    location=PROJECT_REGION
                ),
            ]
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
gemini_api_key_version = gcp.secretmanager.SecretVersion(
    f"debit-{ENV}-secret-version-gemini-api-key",
    secret=gemini_api_key.id,
    secret_data=GEMINI_API_KEY,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("gemini_api_key", gemini_api_key.name)

# Create Cloudflare R2 access key id
cloudflare_r2_access_key = gcp.secretmanager.Secret(
    f"debit-{ENV}-secret-r2-access-key-id",
    secret_id=f"debit-{ENV}-r2-access-key-id",
    replication=gcp.secretmanager.SecretReplicationArgs(
        user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
            replicas=[
                gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                    location=PROJECT_REGION
                ),
            ]
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
cloudflare_r2_access_key_version = gcp.secretmanager.SecretVersion(
    f"debit-{ENV}-secret-version-r2-access-key-id",
    secret=cloudflare_r2_access_key.id,
    secret_data=CLOUDFLARE_R2_ACCESS_KEY_ID,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloudflare_r2_access_key", cloudflare_r2_access_key.name)

# Create Cloudflare R2 secret key
cloudflare_r2_secret_key = gcp.secretmanager.Secret(
    f"debit-{ENV}-secret-r2-secret-key",
    secret_id=f"debit-{ENV}-r2-secret-key",
    replication=gcp.secretmanager.SecretReplicationArgs(
        user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
            replicas=[
                gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                    location=PROJECT_REGION
                ),
            ]
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
cloudflare_r2_secret_key_version = gcp.secretmanager.SecretVersion(
    f"debit-{ENV}-secret-version-r2-secret-key",
    secret=cloudflare_r2_secret_key.id,
    secret_data=CLOUDFLARE_R2_SECRET_ACCESS_KEY,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloudflare_r2_secret_key", cloudflare_r2_secret_key.name)

# endregion

# ------------------------------------------------------------------------
# Cloud Storage
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/storage/
# ------------------------------------------------------------------------

# region

# Data extraction
extract_data_bucket = gcp.storage.Bucket(
    f"debit-{ENV}-extract-bucket",
    location=PROJECT_REGION,
    uniform_bucket_level_access=True,
    force_destroy=IS_TEST,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extract_data_bucket_name", extract_data_bucket.name)
pulumi.export("extract_data_bucket_url", extract_data_bucket.url)

# Data transformation
transform_data_bucket = gcp.storage.Bucket(
    f"debit-{ENV}-transform-bucket",
    location=PROJECT_REGION,
    uniform_bucket_level_access=True,
    force_destroy=IS_TEST,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("transform_data_bucket_name", transform_data_bucket.name)
pulumi.export("transform_data_bucket_url", transform_data_bucket.url)

# endregion

# ------------------------------------------------------------------------
# Cloud SQL
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/sql/
# ------------------------------------------------------------------------

# region

pipeline_db = gcp.sql.DatabaseInstance(
    f"debit-{ENV}-db-pipeline",
    region=PROJECT_REGION,
    database_version="POSTGRES_17",
    settings=gcp.sql.DatabaseInstanceSettingsArgs(
        edition="ENTERPRISE", tier="db-custom-1-3840"
    ),
    deletion_protection=False,
    root_password=postgres_password_version.secret_data.apply(lambda val: val),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("pipeline_db_name", pipeline_db.name)

# endregion

# ------------------------------------------------------------------------
# Artifact Registry
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/artifactregistry/
# ------------------------------------------------------------------------

# region

# Create a Docker image repository for data extraction with a browser
extraction_repo_heavy = gcp.artifactregistry.Repository(
    f"debit-{ENV}-repo-extract-heavy",
    repository_id=f"debit-{ENV}-repo-extract-heavy",
    location=PROJECT_REGION,
    description="Holds Docker images for data extraction using browser automation.",
    cleanup_policies=[
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id="keep-heavy-latest",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1, package_name_prefixes=[]
            ),
        )
    ],
    format="DOCKER",
    docker_config=gcp.artifactregistry.RepositoryDockerConfigArgs(
        immutable_tags=False
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_repo_heavy", extraction_repo_heavy.name)

# Build and push "heavy" image
heavy_extract_image = docker.Image(
    f"debit-{ENV}-image-extract-heavy",
    image_name=extraction_repo_heavy.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/heavy"
    ),
    build=docker.DockerBuildArgs(
        context=EXTRACT_DIR.as_posix(),
        dockerfile=(EXTRACT_DIR / "Dockerfile.heavy").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_heavy_image", heavy_extract_image.image_name)

# Create a Docker image repository for data extraction without a browser
extraction_repo_light = gcp.artifactregistry.Repository(
    f"debit-{ENV}-repo-extract-light",
    repository_id=f"debit-{ENV}-repo-extract-light",
    location=PROJECT_REGION,
    description="Holds Docker images for data extraction without browser automation.",
    cleanup_policies=[
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id="keep-light-latest",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1, package_name_prefixes=[]
            ),
        ),
    ],
    format="DOCKER",
    docker_config=gcp.artifactregistry.RepositoryDockerConfigArgs(
        immutable_tags=False
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_repo_light", extraction_repo_light.name)

# Build and push "light" image
light_extract_image = docker.Image(
    f"debit-{ENV}-image-extract-light",
    image_name=extraction_repo_light.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/light"
    ),
    build=docker.DockerBuildArgs(
        context=EXTRACT_DIR.as_posix(),
        dockerfile=(EXTRACT_DIR / "Dockerfile.light").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_light_image", light_extract_image.image_name)

# Create a Docker image repository for data cleaning
clean_repo = gcp.artifactregistry.Repository(
    f"debit-{ENV}-repo-clean",
    repository_id=f"debit-{ENV}-repo-clean",
    location=PROJECT_REGION,
    description="Holds Docker images for cleaning scraped project data.",
    cleanup_policies=[
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id="keep-clean-latest",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1, package_name_prefixes=[]
            ),
        ),
    ],
    format="DOCKER",
    docker_config=gcp.artifactregistry.RepositoryDockerConfigArgs(
        immutable_tags=False
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("clean_repo", clean_repo.name)

# Build and push data cleaning image
clean_image = docker.Image(
    f"debit-{ENV}-image-clean",
    image_name=clean_repo.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/cleaning-pipeline"
    ),
    build=docker.DockerBuildArgs(
        context=TRANSFORM_DIR.as_posix(),
        dockerfile=(TRANSFORM_DIR / "Dockerfile").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_light_image", light_extract_image.image_name)

# Create a Docker image repository for data mapping
map_repo = gcp.artifactregistry.Repository(
    f"debit-{ENV}-repo-map",
    repository_id=f"debit-{ENV}-repo-map",
    location=PROJECT_REGION,
    description="Holds Docker images for mapping cleaned project data.",
    cleanup_policies=[
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id="keep-mapped-latest",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1, package_name_prefixes=[]
            ),
        ),
    ],
    format="DOCKER",
    docker_config=gcp.artifactregistry.RepositoryDockerConfigArgs(
        immutable_tags=False
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("map_repo", map_repo.name)

# Build and push data mapping image
map_image = docker.Image(
    f"debit-{ENV}-image-map",
    image_name=map_repo.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/mapping-pipeline"
    ),
    build=docker.DockerBuildArgs(
        context=MAPPING_DIR.as_posix(),
        dockerfile=(MAPPING_DIR / "Dockerfile").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("map_image", map_image.image_name)

# endregion

# ------------------------------------------------------------------------
# Service Account
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/serviceaccount/
# ------------------------------------------------------------------------

# region

# CLOUD STORAGE

# Get reference to current cloud project
project = gcp.organizations.get_project()

# Build reference to default storage service account
storage_service_account_member = f"serviceAccount:service-{project.number}@gs-project-accounts.iam.gserviceaccount.com"

# Grant access to PubSub
gcp.projects.IAMMember(
    f"debit-{ENV}-stg-pub-access",
    project=PROJECT_ID,
    role="roles/pubsub.publisher",
    member=storage_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# CLOUD RUN

# Configure custom service account for Cloud Run
cloud_run_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-cloudrun",
    account_id=f"debit-{ENV}-sa-cloudrun",
    display_name="Cloud Run Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloud_run_service_account", cloud_run_service_account.email)

# Store reference to service account email
cloud_run_service_account_member = cloud_run_service_account.email.apply(
    lambda email: f"serviceAccount:{email}"
)

# Grant account access to secrets
for idx, secret_id in enumerate(
    (
        cloudflare_r2_access_key.secret_id,
        cloudflare_r2_secret_key.secret_id,
        django_secret.secret_id,
        gemini_api_key.secret_id,
        postgres_password.secret_id,
    )
):
    gcp.secretmanager.SecretIamMember(
        f"debit-{ENV}-run-sct-access-{idx}",
        secret_id=secret_id,
        role="roles/secretmanager.secretAccessor",
        member=cloud_run_service_account_member,
        opts=pulumi.ResourceOptions(
            depends_on=enabled_services, provider=gcp_provider
        ),
    )

# Grant account access to Cloud SQL
gcp.projects.IAMMember(
    f"debit-{ENV}-run-sql-access",
    project=PROJECT_ID,
    role="roles/cloudsql.client",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant account access to data extraction Cloud Storage bucket
gcp.storage.BucketIAMMember(
    f"debit-{ENV}-run-extractbucket-access",
    bucket=extract_data_bucket.name,
    role="roles/storage.objectAdmin",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant account access to data cleaning Cloud Storage bucket
gcp.storage.BucketIAMMember(
    f"debit-{ENV}-run-cleanbucket-access",
    bucket=transform_data_bucket.name,
    role="roles/storage.objectAdmin",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant account access to Cloud Tasks
gcp.projects.IAMMember(
    f"debit-{ENV}-run-tsk-access",
    project=PROJECT_ID,
    role="roles/cloudtasks.admin",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant account access to Artifact Registry
gcp.projects.IAMMember(
    f"debit-{ENV}-run-reg-access",
    project=PROJECT_ID,
    role="roles/artifactregistry.reader",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# CLOUD TASKS

# Configure custom service account for Cloud Tasks
cloud_tasks_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-tasks",
    account_id=f"debit-{ENV}-sa-tasks",
    display_name="Cloud Tasks Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloud_tasks_service_account", cloud_tasks_service_account.email)

# Store reference to service account email
cloud_tasks_service_account_member = cloud_tasks_service_account.email.apply(
    lambda email: f"serviceAccount:{email}"
)

# Grant Cloud Run service account permission to impersonate Cloud Tasks service account
gcp.serviceaccount.IAMMember(
    f"debit-{ENV}-run-tasks-impersonate",
    service_account_id=cloud_tasks_service_account.name,
    role="roles/iam.serviceAccountUser",
    member=cloud_run_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# CLOUD WORKFLOWS

# Configure custom service account for Cloud Workflows
cloud_workflow_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-flows",
    account_id=f"debit-{ENV}-sa-flows",
    display_name="Cloud Workflow Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export(
    "cloud_workflow_service_account", cloud_workflow_service_account.email
)

# Store reference to service account email
cloud_workflow_service_account_member = (
    cloud_workflow_service_account.email.apply(
        lambda email: f"serviceAccount:{email}"
    )
)

# Grant Cloud Workflow permission to invoke Cloud Run Jobs
gcp.projects.IAMMember(
    f"debit-{ENV}-flows-run-access",
    project=PROJECT_ID,
    role="roles/run.developer",
    member=cloud_workflow_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant Cloud Workflow permission to invoke Cloud SQL commands
gcp.projects.IAMMember(
    f"debit-{ENV}-flows-sql-access",
    project=PROJECT_ID,
    role="roles/cloudsql.admin",
    member=cloud_workflow_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant DB instance's service account object admin permissions on data bucket
gcp.storage.BucketIAMMember(
    f"debit-{ENV}-db-stg-access",
    bucket=extract_data_bucket.name,
    role="roles/storage.objectAdmin",
    member=pulumi.Output.concat(
        "serviceAccount:", pipeline_db.service_account_email_address
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[*enabled_services, pipeline_db], provider=gcp_provider
    ),
)
pulumi.export(
    "pipeline_db_service_account_email_address",
    pipeline_db.service_account_email_address,
)

# CLOUD SCHEDULER

# Configure custom service account for Cloud Scheduler
cloud_scheduler_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-scheduler",
    account_id=f"debit-{ENV}-sa-scheduler",
    display_name="Cloud Scheduler Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export(
    "cloud_scheduler_service_account", cloud_scheduler_service_account.email
)

# Store reference to service account email
cloud_scheduler_service_account_member = (
    cloud_scheduler_service_account.email.apply(
        lambda email: f"serviceAccount:{email}"
    )
)

# Grant Cloud Scheduler service account permission to invoke Cloud Workflow
gcp.projects.IAMMember(
    f"debit-{ENV}-sch-flows-access",
    project=PROJECT_ID,
    role="roles/workflows.invoker",
    member=cloud_scheduler_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# EVENTARC

# Create a service account for Eventarc
eventarc_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-eventarc",
    account_id=f"debit-{ENV}-sa-eventarc",
    display_name="Eventarc Trigger Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("eventarc_service_account", eventarc_service_account.email)

# Store reference to service account email
eventarc_service_account_member = eventarc_service_account.email.apply(
    lambda email: f"serviceAccount:{email}"
)

# Grant the service account permission to invoke workflows
gcp.projects.IAMMember(
    f"debit-{ENV}-arc-flows-access",
    project=PROJECT_ID,
    role="roles/workflows.invoker",
    member=eventarc_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant Eventarc event receiver role
gcp.projects.IAMMember(
    f"debit-{ENV}-arc-receiver",
    project=PROJECT_ID,
    role="roles/eventarc.eventReceiver",
    member=eventarc_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# endregion

# ------------------------------------------------------------------------
# Cloud Run
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/cloudrunv2/
# ------------------------------------------------------------------------

# region

# Intialize variables shared across Cloud Run services
shared_template_args = dict(
    service_account=cloud_run_service_account.email,
    timeout="900s",
    volumes=[
        gcp.cloudrunv2.ServiceTemplateVolumeArgs(
            name="cloudsql",
            cloud_sql_instance=gcp.cloudrunv2.ServiceTemplateVolumeCloudSqlInstanceArgs(
                instances=[pipeline_db.connection_name],
            ),
        )
    ],
)
shared_template_container_args = dict(
    envs=[
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="DJANGO_ALLOWED_HOST", value=DJANGO_ALLOWED_HOST
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="DJANGO_SECRET_KEY",
            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                    secret=django_secret.secret_id,
                    version="latest",
                )
            ),
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="DJANGO_SETTINGS_MODULE",
            value=DJANGO_SETTINGS_MODULE,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="ENV",
            value="prod" if ENV == "p" else "test",
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="GEMINI_API_KEY",
            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                    secret=gemini_api_key.secret_id,
                    version="latest",
                )
            ),
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="GOOGLE_CLOUD_PROJECT_ID",
            value=PROJECT_ID,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="GOOGLE_CLOUD_PROJECT_REGION",
            value=PROJECT_REGION,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="MAX_TASK_RETRIES",
            value=EXTRACTION_PIPELINE_MAX_RETRIES,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="MAX_WAIT_IN_MINUTES",
            value=EXTRACTION_PIPELINE_MAX_WAIT,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="POLLING_INTERVAL_IN_MINUTES",
            value=EXTRACTION_PIPELINE_POLLING_INTERVAL,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="POSTGRES_DB",
            value=POSTGRES_DB,
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="POSTGRES_HOST",
            value=pipeline_db.connection_name.apply(
                lambda name: f"/cloudsql/{name}"
            ),
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="POSTGRES_PASSWORD",
            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                    secret=postgres_password.secret_id,
                    version="latest",
                )
            ),
        ),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="POSTGRES_USER",
            value=POSTGRES_USER,
        ),
    ],
    ports=gcp.cloudrunv2.ServiceTemplateContainerPortsArgs(
        container_port=DJANGO_PORT,
    ),
    startup_probe=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeArgs(
        http_get=gcp.cloudrunv2.ServiceTemplateContainerStartupProbeHttpGetArgs(
            path="/",
            port=DJANGO_PORT,
        ),
        initial_delay_seconds=0,
        period_seconds=30,
        timeout_seconds=1,
        failure_threshold=10,
    ),
    volume_mounts=[
        gcp.cloudrunv2.ServiceTemplateContainerVolumeMountArgs(
            mount_path="/cloudsql",
            name="cloudsql",
        )
    ],
)

# Create Cloud Run service running data extraction pipeline with browser
heavy_cloud_run_service = gcp.cloudrunv2.Service(
    f"debit-{ENV}-runsvc-heavy",
    deletion_protection=False,
    ingress="INGRESS_TRAFFIC_INTERNAL_ONLY",
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=heavy_extract_image.image_name,
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    cpu_idle=True, limits={"memory": "4Gi", "cpu": "1"}
                ),
                **shared_template_container_args,
            )
        ],
        max_instance_request_concurrency=4,
        scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
            min_instance_count=0, max_instance_count=2
        ),
        **shared_template_args,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("heavy_cloud_run_service", heavy_cloud_run_service.name)

# Create Cloud Run service running data extraction pipeline without browser
light_cloud_run_service = gcp.cloudrunv2.Service(
    f"debit-{ENV}-runsvc-light",
    deletion_protection=False,
    ingress="INGRESS_TRAFFIC_INTERNAL_ONLY",
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=light_extract_image.image_name,
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    cpu_idle=True, limits={"memory": "1Gi", "cpu": "1"}
                ),
                **shared_template_container_args,
            )
        ],
        max_instance_request_concurrency=10,
        scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
            min_instance_count=0, max_instance_count=5
        ),
        **shared_template_args,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("light_cloud_run_service", light_cloud_run_service.name)

# Create Cloud Run Job serving as orchestrator
orchestrate_cloud_run_job = gcp.cloudrunv2.Job(
    f"debit-{ENV}-runjob-extract",
    deletion_protection=False,
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.JobTemplateArgs(
        parallelism=1,
        template=gcp.cloudrunv2.JobTemplateTemplateArgs(
            containers=[
                gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                    args=["bash", "setup.sh", "--migrate", "--extract-data"],
                    envs=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_ALLOWED_HOST",
                            value=DJANGO_ALLOWED_HOST,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_SECRET_KEY",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=django_secret.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_SETTINGS_MODULE",
                            value=DJANGO_SETTINGS_MODULE,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="ENV",
                            value="prod" if ENV == "p" else "test",
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="GEMINI_API_KEY",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=gemini_api_key.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="GOOGLE_CLOUD_PROJECT_ID",
                            value=PROJECT_ID,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="GOOGLE_CLOUD_PROJECT_REGION",
                            value=PROJECT_REGION,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="MAX_TASK_RETRIES",
                            value=EXTRACTION_PIPELINE_MAX_RETRIES,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="MAX_WAIT_IN_MINUTES",
                            value=EXTRACTION_PIPELINE_MAX_WAIT,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POLLING_INTERVAL_IN_MINUTES",
                            value=EXTRACTION_PIPELINE_POLLING_INTERVAL,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_DB",
                            value=POSTGRES_DB,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_HOST",
                            value=pipeline_db.connection_name.apply(
                                lambda name: f"/cloudsql/{name}"
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_PASSWORD",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=postgres_password.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_USER",
                            value=POSTGRES_USER,
                        ),
                    ],
                    image=light_extract_image.image_name,
                    ports=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerPortArgs(
                            container_port=DJANGO_PORT
                        )
                    ],
                    resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                        limits={"memory": "512Mi", "cpu": "1"}
                    ),
                    volume_mounts=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerVolumeMountArgs(
                            mount_path="/cloudsql",
                            name="cloudsql",
                        )
                    ],
                )
            ],
            service_account=cloud_run_service_account.email,
            timeout="172800s",
            volumes=[
                gcp.cloudrunv2.JobTemplateTemplateVolumeArgs(
                    name="cloudsql",
                    cloud_sql_instance=gcp.cloudrunv2.JobTemplateTemplateVolumeCloudSqlInstanceArgs(
                        instances=[pipeline_db.connection_name],
                    ),
                )
            ],
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("orchestrate_cloud_run_job", orchestrate_cloud_run_job.name)

# Create Cloud Run Job for data truncation
truncate_cloud_run_job = gcp.cloudrunv2.Job(
    f"debit-{ENV}-runjob-truncate",
    deletion_protection=False,
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.JobTemplateArgs(
        parallelism=1,
        template=gcp.cloudrunv2.JobTemplateTemplateArgs(
            containers=[
                gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                    args=["bash", "setup.sh", "--truncate"],
                    envs=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_ALLOWED_HOST",
                            value=DJANGO_ALLOWED_HOST,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_SECRET_KEY",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=django_secret.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="DJANGO_SETTINGS_MODULE",
                            value=DJANGO_SETTINGS_MODULE,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="ENV",
                            value="prod" if ENV == "p" else "test",
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="GOOGLE_CLOUD_PROJECT_ID",
                            value=PROJECT_ID,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="GOOGLE_CLOUD_PROJECT_REGION",
                            value=PROJECT_REGION,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_DB",
                            value=POSTGRES_DB,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_HOST",
                            value=pipeline_db.connection_name.apply(
                                lambda name: f"/cloudsql/{name}"
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_PASSWORD",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=postgres_password.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="POSTGRES_USER",
                            value=POSTGRES_USER,
                        ),
                    ],
                    image=light_extract_image.image_name,
                    ports=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerPortArgs(
                            container_port=DJANGO_PORT
                        )
                    ],
                    resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                        limits={"memory": "512Mi", "cpu": "1"}
                    ),
                    volume_mounts=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerVolumeMountArgs(
                            mount_path="/cloudsql",
                            name="cloudsql",
                        )
                    ],
                )
            ],
            service_account=cloud_run_service_account.email,
            timeout="86400s",
            volumes=[
                gcp.cloudrunv2.JobTemplateTemplateVolumeArgs(
                    name="cloudsql",
                    cloud_sql_instance=gcp.cloudrunv2.JobTemplateTemplateVolumeCloudSqlInstanceArgs(
                        instances=[pipeline_db.connection_name],
                    ),
                )
            ],
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("truncate_cloud_run_job", truncate_cloud_run_job.name)

# Create Cloud Run Job for data cleaning
clean_cloud_run_job = gcp.cloudrunv2.Job(
    f"debit-{ENV}-runjob-clean",
    deletion_protection=False,
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.JobTemplateArgs(
        parallelism=1,
        template=gcp.cloudrunv2.JobTemplateTemplateArgs(
            containers=[
                gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                    envs=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="ENV",
                            value="prod" if ENV == "p" else "test",
                        )
                    ],
                    image=clean_image.image_name,
                    resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                        limits={"memory": "4Gi", "cpu": "1"}
                    ),
                )
            ],
            service_account=cloud_run_service_account.email,
            timeout="86400s",
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("clean_cloud_run_job", clean_cloud_run_job.name)

# Create Cloud Run Job for data mapping
map_cloud_run_job = gcp.cloudrunv2.Job(
    f"debit-{ENV}-runjob-map",
    deletion_protection=False,
    launch_stage="BETA",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.JobTemplateArgs(
        parallelism=1,
        template=gcp.cloudrunv2.JobTemplateTemplateArgs(
            containers=[
                gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                    envs=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="CLOUDFLARE_R2_ACCESS_KEY_ID",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=cloudflare_r2_access_key.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="CLOUDFLARE_R2_ENDPOINT_URL",
                            value=CLOUDFLARE_R2_ENDPOINT_URL,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="CLOUDFLARE_R2_SECRET_ACCESS_KEY",
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=cloudflare_r2_secret_key.secret_id,
                                    version="latest",
                                )
                            ),
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="ENV",
                            value="prod" if ENV == "p" else "test",
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="OUTPUT_FILE_MAX_AGE",
                            value=OUTPUT_FILE_MAX_AGE,
                        ),
                        gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                            name="OUTPUT_FILE_TOTAL_MAX_ATTEMPTS",
                            value=OUTPUT_FILE_TOTAL_MAX_ATTEMPTS,
                        ),
                    ],
                    image=map_image.image_name,
                    resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                        limits={"memory": "4Gi", "cpu": "1"}
                    ),
                )
            ],
            service_account=cloud_run_service_account.email,
            timeout="86400s",
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("map_cloud_run_job", map_cloud_run_job.name)

# endregion

# ------------------------------------------------------------------------
# Cloud Tasks
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/cloudtasks/
# ------------------------------------------------------------------------

# region

for idx, config in enumerate(QUEUE_CONFIG):
    # Determine HTTP targets for queue
    chosen_uri = pulumi.Output.from_input(config["requires_chromium"]).apply(
        lambda val: (
            heavy_cloud_run_service.uri if val else light_cloud_run_service.uri
        )
    )
    chosen_host = chosen_uri.apply(
        lambda val: val.replace("https://", "")
        .replace("http://", "")
        .split("/")[0]
    )

    # Create queue
    queue = gcp.cloudtasks.Queue(
        f"debit-{ENV}-{config['source']}-queue",
        location=PROJECT_REGION,
        rate_limits=gcp.cloudtasks.QueueRateLimitsArgs(
            max_concurrent_dispatches=config["max_concurrent_dispatches"],
        ),
        retry_config=gcp.cloudtasks.QueueRetryConfigArgs(
            max_attempts=3,
            max_retry_duration="4s",
            max_backoff="3s",
            min_backoff="2s",
            max_doublings=1,
        ),
        http_target=gcp.cloudtasks.QueueHttpTargetArgs(
            http_method="POST",
            oidc_token=gcp.cloudtasks.QueueHttpTargetOidcTokenArgs(
                service_account_email=cloud_tasks_service_account.email,
                audience=chosen_uri,
            ),
            uri_override=gcp.cloudtasks.QueueHttpTargetUriOverrideArgs(
                scheme="HTTPS",
                host=chosen_host,
                path_override=gcp.cloudtasks.QueueHttpTargetUriOverridePathOverrideArgs(
                    path=DJANGO_API_PATH_DATA_EXTRACTION,
                ),
                uri_override_enforce_mode="ALWAYS",
            ),
        ),
        stackdriver_logging_config=gcp.cloudtasks.QueueStackdriverLoggingConfigArgs(
            sampling_ratio=1.0
        ),
        opts=pulumi.ResourceOptions(
            depends_on=enabled_services, provider=gcp_provider
        ),
    )
    pulumi.export(f"{config['source']}-queue", queue.name)

    # Grant Cloud Run service account permission to enqueue tasks in queue
    gcp.cloudtasks.QueueIamMember(
        f"debit-{ENV}-{config['source']}-enqueuer",
        name=queue.name,
        location=PROJECT_REGION,
        project=PROJECT_ID,
        role="roles/cloudtasks.enqueuer",
        member=cloud_run_service_account_member,
        opts=pulumi.ResourceOptions(
            depends_on=enabled_services, provider=gcp_provider
        ),
    )

# Grant Cloud Tasks service account access to invoke Cloud Run services
gcp.cloudrunv2.ServiceIamMember(
    f"debit-{ENV}-tasks-runheavy",
    name=heavy_cloud_run_service.name,
    location=PROJECT_REGION,
    project=PROJECT_ID,
    role="roles/run.invoker",
    member=cloud_tasks_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

gcp.cloudrunv2.ServiceIamMember(
    f"debit-{ENV}-tasks-runlight",
    name=light_cloud_run_service.name,
    location=PROJECT_REGION,
    project=PROJECT_ID,
    role="roles/run.invoker",
    member=cloud_tasks_service_account_member,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant Cloud Tasks service agent permission to mint OIDC tokens for account
gcp.serviceaccount.IAMMember(
    f"debit-{ENV}-tasks-minter",
    service_account_id=cloud_tasks_service_account.name,
    role="roles/iam.serviceAccountOpenIdTokenCreator",
    member=pulumi.Output.concat(
        "serviceAccount:service-",
        gcp.organizations.get_project().number,
        "@gcp-sa-cloudtasks.iam.gserviceaccount.com",
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# endregion

# ------------------------------------------------------------------------
# Cloud Workflows
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/workflows/
# ------------------------------------------------------------------------

# region

# Create extraction workflow
extraction_workflow = gcp.workflows.Workflow(
    f"debit-{ENV}-flows-extract",
    region=PROJECT_REGION,
    description="Triggers a Cloud Run Job for orchestrating data extraction.",
    service_account=cloud_workflow_service_account.email,
    call_log_level="LOG_ERRORS_ONLY",
    deletion_protection=False,
    source_contents=pulumi.Output.format(
        """
        # Workflow to extract development project data for a given date.
        #
        # Uses the current date in UTC if no YYYY-MM-DD date is provided
        # through the DATE_OVERRIDE environment variable. Starts a database
        # instance to store extracted data and then triggers a Cloud Run job
        # to orchestrate the data extraction. When the job completes, exports
        # the database tables as CSV files to Cloud Storage, truncates the
        # database, and turns the database off.
        #
        # References:
        # - https://cloud.google.com/workflows/docs/reference/googleapis#invoke_a_connector_call
        # - https://cloud.google.com/sql/docs/postgres/admin-api/rest/v1/operations
        # - https://cloud.google.com/sql/docs/postgres/admin-api/rest/v1/instances/export
        # - https://cloud.google.com/workflows/docs/reference/googleapis/sqladmin/v1/instances/export
        # - https://cloud.google.com/workflows/docs/reference/googleapis/sqladmin/v1/instances/patch
        #
        main:
            steps:
                - initializeVariables:
                    assign:
                        - executionId: ${{sys.get_env("GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID")}}
                        - date: ${{if(len(sys.get_env("DATE_OVERRIDE", "")) > 0, sys.get_env("DATE_OVERRIDE"), text.substring(time.format(sys.now()), 0, 10))}}
                        - bucketUrl: {data_bucket_url}
                        - extractDir: ${{bucketUrl + "/extraction/" + date + "/"}}
                        - fileId: ${{date + "-" + executionId}}
                        - jobPrefix: projects/{project_id}/locations/{project_region}/jobs/
                        - orchestrateJobFullName: ${{jobPrefix + "{orchestrate_job_name}"}}
                        - truncateJobFullName: ${{jobPrefix + "{truncate_job_name}"}}
                - startDatabase:
                    call: googleapis.sqladmin.v1.instances.patch
                    args:
                        instance: {cloud_sql_instance_id}
                        project: {project_id}
                        body:
                            settings:
                                activationPolicy: ALWAYS
                    result: startDbOperation
                - extractData:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                        name: ${{orchestrateJobFullName}}
                        body:
                            overrides:
                                containerOverrides:
                                    args:
                                        - bash
                                        - setup.sh
                                        - --migrate
                                        - --extract-data
                                        - --date
                                        - ${{date}}
                        connector_params:
                            timeout: 86400
                    result: extractDataOperation
                - exportJob:
                    call: googleapis.sqladmin.v1.instances.export
                    args:
                        instance: {cloud_sql_instance_id}
                        project: {project_id}
                        body:
                            exportContext:
                                csvExportOptions:
                                    selectQuery: "SELECT * FROM public.extraction_job"
                                    escapeCharacter: "22"
                                    quoteCharacter: "22"
                                    fieldsTerminatedBy: "09"
                                databases:
                                    - {database_name}
                                fileType: CSV
                                kind: sql#exportContext
                                uri: ${{extractDir + "_jobs_" + executionId + ".tsv.gz" }}
                    result: jobOperation
                - exportTasks:
                    call: googleapis.sqladmin.v1.instances.export
                    args:
                        instance: {cloud_sql_instance_id}
                        project: {project_id}
                        body:
                            exportContext:
                                csvExportOptions:
                                    selectQuery: "SELECT * FROM public.extraction_task"
                                    escapeCharacter: "22"
                                    quoteCharacter: "22"
                                    fieldsTerminatedBy: "09"
                                databases:
                                    - {database_name}
                                fileType: CSV
                                kind: sql#exportContext
                                uri: ${{extractDir + "_tasks_" + executionId + ".tsv.gz" }}
                    result: jobOperation
                - exportProjects:
                    call: googleapis.sqladmin.v1.instances.export
                    args:
                        instance: {cloud_sql_instance_id}
                        project: {project_id}
                        body:
                            exportContext:
                                csvExportOptions:
                                    selectQuery: "SELECT * FROM public.extracted_project"
                                    escapeCharacter: "22"
                                    quoteCharacter: "22"
                                    fieldsTerminatedBy: "09"
                                databases:
                                    - {database_name}
                                fileType: CSV
                                kind: sql#exportContext
                                uri: ${{extractDir + "_projects_" + executionId + ".tsv.gz" }}
                    result: projectOperation
                - truncateDatabase:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                        name: ${{truncateJobFullName}}
                        connector_params:
                            timeout: 86400
                    result: truncateDataOperation
                - stopDatabase:
                    call: googleapis.sqladmin.v1.instances.patch
                    args:
                        instance: {cloud_sql_instance_id}
                        project: {project_id}
                        body:
                            settings:
                                activationPolicy: NEVER
                    result: stopDbOperation
        """,
        cloud_sql_instance_id=pipeline_db.name,
        database_name=POSTGRES_DB,
        data_bucket_url=extract_data_bucket.url,
        orchestrate_job_name=orchestrate_cloud_run_job.name,
        truncate_job_name=truncate_cloud_run_job.name,
        project_id=PROJECT_ID,
        project_region=PROJECT_REGION,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_workflow", extraction_workflow.name)

# Create data cleaning workflow
cleaning_workflow = gcp.workflows.Workflow(
    f"debit-{ENV}-flows-clean",
    region=PROJECT_REGION,
    description="Triggers a Cloud Run Job for cleaning scraped project data.",
    service_account=cloud_workflow_service_account.email,
    call_log_level="LOG_ERRORS_ONLY",
    deletion_protection=False,
    source_contents=pulumi.Output.format(
        """
        # Workflow to clean a file of scraped development bank projects.
        #
        # Triggered via Eventarc when a file is written to or updated
        # within the transformed data bucket.
        #
        # References:
        # - https://cloud.google.com/workflows/docs/reference/googleapis/run/v2/projects.locations.jobs/run
        # - https://cloud.google.com/workflows/docs/tutorials/execute-cloud-run-jobs#deploy-workflow
        # - https://googleapis.github.io/google-cloudevents/examples/binary/storage/StorageObjectData-simple.json
        # - https://cloud.google.com/run/docs/tutorials/eventarc
        # - https://cloud.google.com/run/docs/triggering/storage-triggers
        # - https://cloudevents.io/
        # - https://github.com/cloudevents/sdk-python
        #
        main:
            params: [event]
            steps:
                - confirmProjectFile:
                    switch:
                        - condition: ${{ not text.match_regex(event.data.name, "projects") }}
                          return:
                    next: initializeVariables
                - initializeVariables:
                    assign:
                        - inputBucket: ${{ "gs://" + event.data.bucket }}
                        - outputBucket: {output_bucket_url}
                        - objectKey: ${{ event.data.name }}
                        - jobPrefix: projects/{project_id}/locations/{project_region}/jobs/
                        - cleanJobFullName: ${{ jobPrefix + "{clean_job_name}" }}
                - cleanData:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                        name: ${{cleanJobFullName}}
                        body:
                            overrides:
                                containerOverrides:
                                    args:
                                        - ${{ objectKey }}
                                        - --input_bucket
                                        - ${{ inputBucket }}
                                        - --output_bucket
                                        - ${{ outputBucket }}
                                        - --remote
                        connector_params:
                            timeout: 86400
                    result: cleanDataOperation
        """,
        clean_job_name=clean_cloud_run_job.name,
        project_id=PROJECT_ID,
        project_region=PROJECT_REGION,
        output_bucket_url=transform_data_bucket.url,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cleaning_workflow", cleaning_workflow.name)

# Create data mapping workflow
mapping_workflow = gcp.workflows.Workflow(
    f"debit-{ENV}-flows-map",
    region=PROJECT_REGION,
    description="Triggers a Cloud Run Job for mapping cleaned project data.",
    service_account=cloud_workflow_service_account.email,
    call_log_level="LOG_ERRORS_ONLY",
    deletion_protection=False,
    source_contents=pulumi.Output.format(
        """
        # Workflow to map clean development bank projects for the DeBIT website.
        #
        # Triggered via Eventarc when a file is written to
        # or updated within the cleaned data bucket.
        #
        # References:
        # - https://cloud.google.com/workflows/docs/reference/googleapis/run/v2/projects.locations.jobs/run
        # - https://cloud.google.com/workflows/docs/tutorials/execute-cloud-run-jobs#deploy-workflow
        # - https://googleapis.github.io/google-cloudevents/examples/binary/storage/StorageObjectData-simple.json
        # - https://cloud.google.com/run/docs/tutorials/eventarc
        # - https://cloud.google.com/run/docs/triggering/storage-triggers
        # - https://cloudevents.io/
        # - https://github.com/cloudevents/sdk-python
        #
        main:
            params: [event]
            steps:
                - initializeVariables:
                    assign:
                        - inputBucket: ${{ "gs://" + event.data.bucket }}
                        - inputObjectKey: ${{ event.data.name }}
                        - outputBucket: {output_bucket}
                        - outputObjectKey: {output_object_key}
                        - jobPrefix: projects/{project_id}/locations/{project_region}/jobs/
                        - mapJobFullName: ${{ jobPrefix + "{map_job_name}" }}
                - mapData:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                        name: ${{mapJobFullName}}
                        body:
                            overrides:
                                containerOverrides:
                                    args:
                                        - ${{ inputObjectKey }}
                                        - ${{ outputObjectKey }}
                                        - --input_bucket
                                        - ${{ inputBucket }}
                                        - --output_bucket
                                        - ${{ outputBucket }}
                                        - --remote
                        connector_params:
                            timeout: 86400
                    result: mapDataOperation
        """,
        map_job_name=map_cloud_run_job.name,
        output_bucket=CLOUDFLARE_R2_BUCKET_URL,
        output_object_key=OUTPUT_FILE_NAME,
        project_id=PROJECT_ID,
        project_region=PROJECT_REGION,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("mapping_workflow", mapping_workflow.name)

# endregion

# ------------------------------------------------------------------------
# Cloud Scheduler
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/cloudscheduler/
# ------------------------------------------------------------------------

# region

# Create scheduled job
scheduled_job = gcp.cloudscheduler.Job(
    f"debit-{ENV}-sch-extract",
    description="A scheduled job to extract data.",
    region=PROJECT_REGION,
    schedule=EXTRACTION_PIPELINE_SCHEDULE,
    time_zone="Etc/UTC",
    retry_config=gcp.cloudscheduler.JobRetryConfigArgs(
        retry_count=0,
    ),
    http_target=gcp.cloudscheduler.JobHttpTargetArgs(
        body=std.base64encode(input="{}").result,
        headers={"Content-Type": "application/json"},
        http_method="POST",
        uri=pulumi.Output.all(
            extraction_workflow.project,
            extraction_workflow.region,
            extraction_workflow.name,
        ).apply(
            lambda args: "https://workflowexecutions.googleapis.com/v1/projects/{}/locations/{}/workflows/{}/executions".format(
                *args
            )
        ),
        oauth_token=gcp.cloudscheduler.JobHttpTargetOauthTokenArgs(
            service_account_email=cloud_scheduler_service_account.email,
            scope="https://www.googleapis.com/auth/cloud-platform",
        ),
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_scheduled_job", scheduled_job.name)

# endregion

# ------------------------------------------------------------------------
# Eventarc
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/eventarc/
# ------------------------------------------------------------------------

# region

# Create the data cleaning Eventarc trigger
clean_workflow_trigger = gcp.eventarc.Trigger(
    f"debit-{ENV}-trigger-clean",
    name=f"debit-{ENV}-trigger-clean",
    location=PROJECT_REGION,
    matching_criterias=[
        gcp.eventarc.TriggerMatchingCriteriaArgs(
            attribute="type", value="google.cloud.storage.object.v1.finalized"
        ),
        gcp.eventarc.TriggerMatchingCriteriaArgs(
            attribute="bucket", value=extract_data_bucket.name
        ),
    ],
    destination=gcp.eventarc.TriggerDestinationArgs(
        workflow=cleaning_workflow.id
    ),
    service_account=eventarc_service_account.email,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Create the data mapping workflow trigger
map_workflow_trigger = gcp.eventarc.Trigger(
    f"debit-{ENV}-trigger-map",
    name=f"debit-{ENV}-trigger-map",
    location=PROJECT_REGION,
    matching_criterias=[
        gcp.eventarc.TriggerMatchingCriteriaArgs(
            attribute="type", value="google.cloud.storage.object.v1.finalized"
        ),
        gcp.eventarc.TriggerMatchingCriteriaArgs(
            attribute="bucket", value=transform_data_bucket.name
        ),
    ],
    destination=gcp.eventarc.TriggerDestinationArgs(
        workflow=mapping_workflow.id
    ),
    service_account=eventarc_service_account.email,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# endregion
