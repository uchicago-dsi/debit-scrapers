"""A script to automate infrastructure deployment using Pulumi."""

# Third-party imports
import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
import pulumi_std as std

# Application imports
from constants import (
    DJANGO_ALLOWED_HOST,
    DJANGO_PORT,
    DJANGO_SECRET_KEY,
    DJANGO_SETTINGS_MODULE,
    ENV,
    EXTRACTION_PIPELINE_MAX_WAIT,
    EXTRACTION_PIPELINE_POLLING_INTERVAL,
    EXTRACTION_PIPELINE_SCHEDULE,
    GEMINI_API_KEY,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
    PROJECT_ID,
    PROJECT_REGION,
    QUEUE_CONFIG,
    SRC_DIR,
)

# ------------------------------------------------------------------------
# Provider
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/provider/
# ------------------------------------------------------------------------

gcp_provider = gcp.Provider(
    f"debit-{ENV}-gcp-provider",
    default_labels={
        "project": "debit",
        "organization": "idi",
        "environment": "test",
        "managed-by": "uchicago-dsi",
        "automation": "pulumi",
    },
)

# ------------------------------------------------------------------------
# Projects
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/projects/
# ------------------------------------------------------------------------

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

# endregion

# ------------------------------------------------------------------------
# Cloud Storage
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/storage/
# ------------------------------------------------------------------------

# region

data_bucket = gcp.storage.Bucket(
    f"debit-{ENV}-bucket-data",
    location=PROJECT_REGION,
    uniform_bucket_level_access=True,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("data_bucket_name", data_bucket.name)
pulumi.export("data_bucket_url", data_bucket.url)

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
        edition="ENTERPRISE", tier="db-f1-micro"
    ),
    deletion_protection=False,
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
            id="keep-heavy",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1,
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

# Create a Docker image repository for data extraction without a browser
extraction_repo_light = gcp.artifactregistry.Repository(
    f"debit-{ENV}-repo-extract-light",
    repository_id=f"debit-{ENV}-repo-extract-light",
    location=PROJECT_REGION,
    description="Holds Docker images for data extraction without browser automation.",
    cleanup_policies=[
        gcp.artifactregistry.RepositoryCleanupPolicyArgs(
            id="keep-light",
            action="KEEP",
            most_recent_versions=gcp.artifactregistry.RepositoryCleanupPolicyMostRecentVersionsArgs(
                keep_count=1,
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

# Build and push "heavy" image
heavy_extract_image = docker.Image(
    f"debit-{ENV}-image-extract-heavy",
    image_name=extraction_repo_heavy.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/heavy"
    ),
    build=docker.DockerBuildArgs(
        context=SRC_DIR.as_posix(),
        dockerfile=(SRC_DIR / "Dockerfile.heavy").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_heavy_image", heavy_extract_image.image_name)

# Build and push "light" image
light_extract_image = docker.Image(
    f"debit-{ENV}-image-extract-light",
    image_name=extraction_repo_light.repository_id.apply(
        lambda id: f"{PROJECT_REGION}-docker.pkg.dev/{PROJECT_ID}/{id}/light"
    ),
    build=docker.DockerBuildArgs(
        context=SRC_DIR.as_posix(),
        dockerfile=(SRC_DIR / "Dockerfile.light").as_posix(),
        platform="linux/amd64",
    ),
    registry=docker.RegistryArgs(server=f"{PROJECT_REGION}-docker.pkg.dev"),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_light_image", light_extract_image.image_name)

# endregion

# ------------------------------------------------------------------------
# Service Account
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/serviceaccount/
# ------------------------------------------------------------------------

# region

# Configure custom service account for Cloud Run
cloud_run_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-cloudrun",
    display_name="Cloud Run Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloud_run_service_account", cloud_run_service_account.email)

# Store reference to service account email
cloud_run_service_account_email = cloud_run_service_account.email.apply(
    lambda email: f"serviceAccount:{email}"
)

# Grant account access to secrets
for idx, secret_id in enumerate(
    (
        django_secret.secret_id,
        gemini_api_key.secret_id,
        postgres_password.secret_id,
    )
):
    gcp.secretmanager.SecretIamMember(
        f"debit-{ENV}-run-sct-access-{idx}",
        secret_id=secret_id,
        role="roles/secretmanager.secretAccessor",
        member=cloud_run_service_account_email,
        opts=pulumi.ResourceOptions(
            depends_on=enabled_services, provider=gcp_provider
        ),
    )

# Grant account access to Cloud SQL
gcp.projects.IAMBinding(
    f"debit-{ENV}-run-sql-access",
    project=PROJECT_ID,
    role="roles/cloudsql.client",
    members=[cloud_run_service_account_email],
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant account access to Cloud Storage bucket
gcp.storage.BucketIAMMember(
    f"debit-{ENV}-run-stg-access",
    bucket=data_bucket.name,
    role="roles/storage.objectAdmin",
    member=cloud_run_service_account_email,
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Configure custom service account for Cloud Tasks
cloud_tasks_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-tasks",
    display_name="Cloud Tasks Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("cloud_tasks_service_account", cloud_tasks_service_account.email)

# Store reference to service account email
cloud_tasks_service_account_email = cloud_tasks_service_account.email.apply(
    lambda email: f"serviceAccount:{email}"
)

# Configure custom service account for Cloud Scheduler
cloud_scheduler_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-scheduler",
    display_name="Cloud Scheduler Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export(
    "cloud_scheduler_service_account", cloud_scheduler_service_account.email
)

# Store reference to service account email
cloud_scheduler_service_account_email = (
    cloud_scheduler_service_account.email.apply(
        lambda email: f"serviceAccount:{email}"
    )
)

# Configure custom service account for Cloud Workflows
cloud_workflow_service_account = gcp.serviceaccount.Account(
    f"debit-{ENV}-sa-flows",
    display_name="Cloud Workflow Service Account",
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export(
    "cloud_workflow_service_account", cloud_workflow_service_account.email
)

# Store reference to service account email
cloud_workflow_service_account_email = (
    cloud_workflow_service_account.email.apply(
        lambda email: f"serviceAccount:{email}"
    )
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
    volume_mounts=[
        gcp.cloudrunv2.ServiceTemplateContainerVolumeMountArgs(
            name="cloudsql",
            mount_path="/cloudsql",
        )
    ],
)

# Create Cloud Run service running data extraction pipeline with browser
heavy_cloud_run_service = gcp.cloudrunv2.Service(
    f"debit-{ENV}-runsvc-heavy",
    ingress="INGRESS_TRAFFIC_INTERNAL_ONLY",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=heavy_extract_image.image_name,
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={"memory": "4Gi", "cpu": "1"}
                ),
                **shared_template_container_args,
            )
        ],
        max_instance_request_concurrency=2,
        **shared_template_args,
    ),
    traffics=[
        gcp.cloudrunv2.ServiceTrafficArgs(
            percent=100,
        )
    ],
    scaling=gcp.cloudrunv2.ServiceScalingArgs(
        min_instance_count=0,
        manual_instance_count=20,
        scaling_mode="MANUAL",
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("heavy_cloud_run_service", heavy_cloud_run_service.name)

# Create Cloud Run service running data extraction pipeline without browser
light_cloud_run_service = gcp.cloudrunv2.Service(
    f"debit-{ENV}-runsvc-light",
    ingress="INGRESS_TRAFFIC_INTERNAL_ONLY",
    location=PROJECT_REGION,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=light_extract_image.image_name,
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={"memory": "256Mi", "cpu": "1"}
                ),
                **shared_template_container_args,
            )
        ],
        max_instance_request_concurrency=100,
        **shared_template_args,
    ),
    traffics=[
        gcp.cloudrunv2.ServiceTrafficArgs(
            percent=100,
        )
    ],
    scaling=gcp.cloudrunv2.ServiceScalingArgs(
        min_instance_count=0,
        manual_instance_count=20,
        scaling_mode="MANUAL",
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("light_cloud_run_service", light_cloud_run_service.name)

# Create Cloud Run Job serving as orchestrator
orchestrator_cloud_run_job = gcp.cloudrunv2.Job(
    f"debit-{ENV}-runjob-extract",
    location=PROJECT_REGION,
    parallelism=1,
    template=gcp.cloudrunv2.JobTemplateArgs(
        **shared_template_args,
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                envs=[
                    *shared_template_container_args["envs"],
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="MAX_WAIT_IN_MINUTES",
                        value=EXTRACTION_PIPELINE_MAX_WAIT,
                    ),
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="POLLING_INTERVAL_IN_MINUTES",
                        value=EXTRACTION_PIPELINE_POLLING_INTERVAL,
                    ),
                ],
                image=light_extract_image.image_name,
                ports=shared_template_container_args["ports"],
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={"memory": "256Mi", "cpu": "1"}
                ),
                service_account=shared_template_container_args[
                    "service_account"
                ],
                volume_mounts=shared_template_container_args["volume-mounts"],
            )
        ],
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("orchestrator_cloud_run_job", orchestrator_cloud_run_job.name)

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
                    path="/api/v1/gcp/tasks",
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
        member=cloud_run_service_account_email,
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
    member=cloud_tasks_service_account_email,
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
    member=cloud_tasks_service_account_email,
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

extraction_workflow = gcp.workflows.Workflow(
    f"debit-{ENV}-flows-extract",
    region=PROJECT_REGION,
    description="Triggers a Cloud Run Job for orchestrating data extraction.",
    service_account=cloud_workflow_service_account.email,
    call_log_level="LOG_ERRORS_ONLY",
    deletion_protection=False,
    source_contents=pulumi.Output.format(
        """
        main:
            steps:
                - extract_data:
                    call: http.post
                    args:
                        auth:
                            type: OAuth2
                        body: {{}}
                        url: "https://run.googleapis.com/v2/projects/{project_id}/locations/{project_region}/jobs/{job_name}:run"
                    result: response
        """,
        project_id=PROJECT_ID,
        project_region=PROJECT_REGION,
        job_name=orchestrator_cloud_run_job.name,
    ),
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)
pulumi.export("extraction_workflow", extraction_workflow.name)

# Grant Cloud Workflow permission to invoke Cloud Run Jobs
gcp.projects.IAMBinding(
    f"debit-{ENV}-flows-run-access",
    project=PROJECT_ID,
    role="roles/run.developer",
    members=[cloud_workflow_service_account_email],
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# Grant Cloud Scheduler service account permission to invoke Cloud Workflow
gcp.projects.IAMBinding(
    f"debit-{ENV}-sch-flows-access",
    project=PROJECT_ID,
    role="roles/workflows.invoker",
    members=[cloud_scheduler_service_account_email],
    opts=pulumi.ResourceOptions(
        depends_on=enabled_services, provider=gcp_provider
    ),
)

# endregion

# ------------------------------------------------------------------------
# Cloud Scheduler
# See: https://www.pulumi.com/registry/packages/gcp/api-docs/cloudscheduler/
# ------------------------------------------------------------------------

# region

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
