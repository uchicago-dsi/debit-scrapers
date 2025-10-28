# Google Cloud Platform (GCP)

Scripts for deploying backend infrastructure to GCP using Pulumi and the gcloud CLI.

## Manual Setup

Before running the scripts, a Pulumi stack must be created. In the current implementation, infrastructure is persisted to a Google Cloud Storage bucket to save costs (as opposed to maintaining a subscription to Pulumi Cloud), and the setup process is implemented manually.

**(1) Log into GCP**

Open a new terminal and run the following command to authenticate with Google Cloud using the gcloud CLI.

```
gcloud auth application-default login
```

The command will launch a new browser window and open to the login page. Select your identity and then click "Continue" until you reach the success message, "You are now authenticated with the gcloud CLI!" Then close the browser and return to your terminal.

**(2) Set GCP Project**

Navigate to the GitHub repository's settings and find DeBIT's Google Cloud Project id in the list of configured variables.

Run the following command to confirm that the same id is returned, indicating that your active project is DeBIT:

```
gcloud config get-value project
```

If you have a different project selected, change to DeBIT by entering:

```
gcloud config set project PROJECT_ID
```

**(3) Create Infrastructure Bucket**

Using either the Cloud Console or gcloud CLI, create a Cloud Storage bucket with default permissions to hold the Pulumi infrastructure state. The following bucket names are expected for each environment:

_Test_

```
debit-t-pulumi-infra
```

_Prod_

```
debit-p-pulumi-infra
```

**(4) Log into Infrastructure Bucket**

Run the following command to log into the configured storage bucket:

_Test_

```
pulumi login gs://debit-t-pulumi-infra
```

_Prod_

```
pulumi login gs://debit-p-pulumi-infra
```

For more information on this login method, please consult the **[Pulumi documentation](https://www.pulumi.com/docs/iac/concepts/state-and-backends/)**.

**(4) Create New Stack**

Run the following command to create a new stack in the bucket:

_Test_

```
pulumi stack init test
```

_Prod_

```
pulumi stack init prod
```

At this point, you will be given the option to enter a passphrase to protect configuration/secrets. At present, no passphrase is expected, but if you choose to enter one, you must save a new secret in the corresponding GitHub environment (i.e., `test` or `prod`) and update the GitHub Action definition to pass the passphrase to the Pulumi deployment steps.

**(5) Set Stack Region**

Run the following commands to set the GCP region of the new stack:

_Test_

```
pulumi stack select test
pulumi config set gcp:region us-central1
```

_Prod_

```
pulumi stack select prod
pulumi config set gcp:region us-central1
```

## Structure

### one-time

When seeding a new GCP project, the bash script in the `one-time` directory should be executed first. The script creates a service account with the permissions necessary to deploy and manage resources using Pulumi and then permits GitHub Actions to impersonate that account as a Workload Identity. When the `deploy-gcp.yaml` GitHub Action is triggered, the service account will successfully authenticate to Google Cloud using OIDC tokens.

## persistent

The Python scripts and YAML file in `persistent` define the infrastructure that runs the logic of the data pipeline. There are two Pulumi stacks, `test` and `prod`, which are configured on GitHub to deploy upon pushes to the `test` and `main` branches, respectively.
