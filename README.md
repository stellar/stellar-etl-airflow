# stellar-etl-airflow

This repository contains the Airflow DAGs for the [Stellar ETL](https://github.com/stellar/stellar-etl) project. These DAGs provide a workflow for exporting data from the Stellar network and uploading the data into BigQuery.

## **Table of Contents**

- [Installation and Setup](#installation-and-setup)
  - [Airflow Variables Explanation](#airflow-variables-explanation)
    - [Normal Variables](#normal-variables)
    - [Kubernetes Specific Variables](#kubernetes-specific-variables)
- [Execution Procedures](#execution-procedures)
  - [Starting Up](#starting-up)
  - [Handling Failures](#handling-failures)
    - [Clearing Failures](#clearing-failures)
- [Understanding the Setup](#understanding-the-setup)
  - [DAG Diagrams](#dag-diagrams)
    - [Public DAGs](#public-dags)
      - [History Table Export DAG](#history-table-export-dag)
      - [State Table Export DAG](#state-table-export-dag)
      - [DBT Enriched Base Tables DAG](#dbt-enriched-base-tables-dag)
    - [SDF Internal DAGs](#sdf-internal-dags)
      - [Sandbox update DAG](#sandbox-update-dag)
      - [Cleanup metadata DAG](#cleanup-metadata-dag)
      - [Partner Pipeline DAG](#partner-pipeline-dag)
      - [DBT SDF Marts DAG](#dbt-sdf-marts-dag)
      - [Daily Euro OHLC DAG](#daily-euro-ohlc-dag)
      - [Audit Log DAG](#audit-log-dag)
  - [Task Explanations](#task-explanations)
    - [build_time_task](#build_time_task)
    - [build_export_task](#build_export_task)
    - [build_gcs_to_bq_task](#build_gcs_to_bq_task)
    - [build_del_ins_from_gcs_to_bq_task](#build_del_ins_from_gcs_to_bq_task)
    - [build_apply_gcs_changes_to_bq_task](#build_apply_gcs_changes_to_bq_task)
    - [build_batch_stats](#build_batch_stats)
    - [bq_insert_job_task](#bq_insert_job_task)
    - [cross_dependency_task](#cross_dependency_task)
    - [build_delete_data_task](#build_delete_data_task)
    - [build_dbt_task](#build_dbt_task)
    - [build_elementary_slack_alert_task](#build_elementary_slack_alert_task)
- [Further Development](#further-development)
  - [Extensions](#extensions)
    - [Pre-commit Git hook scripts](#pre-commit-git-hook-scripts)
    - [Adding New DAGs](#adding-new-dags)
    - [Adding tasks to existing DAGs](#adding-tasks-to-existing-dags)
    - [Adding New Tasks](#adding-new-tasks)
  - [Testing Changes](#testing-changes)

<br>

---

# Installation and Setup

<br>

---

### **Step 1. Setup the Cloud SDK**

- Download the [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstart#installing_the_latest_version).
- [Initialize the Cloud SDK](https://cloud.google.com/sdk/docs/quickstart#initializing_the) and login to your Google account

### **Step 2. Create Google Project**

- Login to the [Google Cloud Console](https://console.cloud.google.com/cloud-resource-manager)
- Create a new [Google Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#creating_a_project) or use an existing project

  ***NOTE:*** The project name you choose corresponds to the Airflow variable "bq_project".

### **Step 3. Create BigQuery Dataset**

- Log in to Google [BigQuery](https://cloud.google.com/bigquery)
- [Create](https://cloud.google.com/bigquery/docs/datasets#create-dataset) a new dataset with the desired name or use an existing dataset

  ***NOTE:*** The dataset name you choose corresponds to the Airflow variable "bq_dataset".

### **Step 4. Create Google Cloud Storage bucket**

- Open the [Cloud Storage browser](https://console.cloud.google.com/storage/browser)

- [Create](https://cloud.google.com/storage/docs/creating-buckets) a new Google Storage bucket that will store exported files or use a GCS bucket created automatically by the composer environment in step 5. You can check this bucket's name by clicking on the DAGs folder link in the Composer section of the Cloud Console.

  ***NOTE:*** The bucket name corresponds to the Airflow variable "gcs_exported_data_bucket_name".

***WARNING:*** Make sure that you adhere to the [location requirements](https://cloud.google.com/bigquery/docs/batch-loading-data#data-locations) for Cloud Storage buckets and BigQuery datasets. Otherwise, it will not be possible to upload data to BigQuery.

---

### **Step 5. Cloud Composer**

Cloud Composer is the preferred method of deployment. [Cloud Composer](https://cloud.google.com/composer) is a managed service used for Airflow deployment that provides much of the infrastructure required to host an Airflow instance. The steps for setting up a Cloud Composer environment are detailed below.

*Note:* more general instructions for setting up the environment can be found [here](https://cloud.google.com/composer/docs/composer-2/create-environments)

---

#### **a. Create a service account**

Setting up a composer environment requires a service account. Generate a [service account](https://cloud.google.com/iam/docs/service-accounts-create) that has following access:

- Composer Worker
- Editor
- Secret Manager Secret Accessor
- Storage Admin
- Storage Insights Collector Service
- Storage Object Admin
- Cloud Composer v2 API Service Agent Extension

#### **b. Create Google Cloud Composer environment**

Create a new Cloud Composer environment using the [UI](https://console.cloud.google.com/composer/environments/create) or by following the setup instructions in [Create Cloud Composer environments](https://cloud.google.com/composer/docs/how-to/managing/creating) Cloud Composer may take 20-25 mins to setup the environment. Once the process is finished, you can view the environment by going to the [Composer section of the Cloud Console](https://console.cloud.google.com/composer/environments).

*Note:* Cloud Composer 1 is in the post-maintenance mode. Google does not release any further updates to Cloud Composer 1, including new versions of Airflow, bugfixes, and security updates. [Composer 1](https://cloud.google.com/composer/docs/concepts/overview)

*Note*: The python version must be 3, and the image must be `composer-2.7.1-airflow-2.6.3` or later. GCP deprecates support for older versions of composer and airflow. It is recommended that you select a stable, latest version to avoid an environment upgrade. See [the command reference page](https://cloud.google.com/sdk/gcloud/reference/composer/environments/create) for a detailed list of parameters.

*TROUBLESHOOTING:* If the environment creation fails because the "Composer Backend timed out" try disabling and enabling the Cloud Composer API. If the creation fails again, try creating a service account with `Owner` permissions and use it to create the Composer environment.

#### **c. Upload DAGs and Schemas to Cloud Composer**

After the environment is created, select the environment and navigate to the environment configuration tab. Look for the value under the DAGs **folder**. It will be of the form `gs://airflow_bucket/dags`. The `airflow_bucket` value will be used in this step and the next. Run the command below in order to upload the DAGs and schemas to your Airflow bucket.

```
> bash upload_static_to_gcs.sh <airflow_bucket>
```

#### **d. Setup following in composer environment**

- Airflow configuration overrides
- Environment Variables
- Labels
- PYPI packages

#### **e. Add airflow Variables and connections in airflow UI**

Click the Admin tab, then Connections. Click create, then:

- Set the `Conn Id` field to `google_cloud_platform_connection`.
- Set the `Conn Type` to `Google Cloud Platform`.
- Set the `Project Id` to your project id
- Set the `Keyfile Path` to `<api_key_path>`.
- The `<api_key_path>` should be the same as the Airflow variable `api_key_path`.

Next, add the Airflow variables. Click the Admin tab, then Variables. Click the `Choose file` button, select your variables file, and click import variables.

The `airflow_variables_*.txt` files provide a set of default values for variables.

Afterwards, you can navigate to the Airflow UI for your Cloud Composer environment. To do so, navigate to the [Composer section of the Cloud Console](https://console.cloud.google.com/composer/environments), and click the link under `Airflow webserver`. Then, pause the DAGs by clicking the on/off toggle to the left of their names. DAGs should remain paused until you have finished setting up the environment. At this point, DAGs should render successfully in the Airflow UI. You may see issues related to secret not imported successfully. You will learn how to setup secrets in Step 6.a

### **Step 6. Setup kubernetes**

Log into google cloud shell and run following command

```
gcloud container clusters list
## above list set of active kubernetes clusters

gcloud container clusters get-credentials {your cluster name} --zone us-central1
## Above switches the context of the shell to your cluster
```

#### **a. Setup secrets**

##### Private docker registry auth secrets

If you want to pull an image from a private docker registry to use in `KubernetesPodOperator` in airflow you will need to add auth json credentials to kubernetes and the service account.

- In the cloud shell, create a docker config file - `docker-config.json` . This will look like following:

```
{
    "auths": {
        "https://index.docker.io/v1/": {
            "auth": "xxx.."
        }
    }
}
```

- Create the kubernetes secret(eg secret name: google-docker-auth) from auth json

```
kubectl create secret generic <secret name> \
    --from-file=.dockerconfigjson=<path/to/.docker-config.json> \
    --type=kubernetes.io/dockerconfigjson
```

##### Other secrets

Similarly you can create other secrets required by your DAGs. Eg: `retool_api_key`

```
kubectl create secret generic retool_api_key --from-literal=token=xxada
```

#### **b. Create namespace and service accounts to authenticate tasks**

There are a few extra hoops to jump through to configure Workload Identity, so that `export` tasks have permissions to upload files to GCS.

You will require two kinds of service accounts: k8s service account and a google service account. Any permission to access GCP will be given to google service account and then we will bind google service account with k8s service account. Kubernetes access k8s service account directly and google service account indirectly. Steps taken from this [doc](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#authenticating_to).

```
# Create a namespace
kubectl create ns {example-namespace} # eg: hubble-composer, this corresponds to k8s_namespace airflow variable

# Create k8s service account
kubectl create serviceaccount {example-k8-service-account} --namespace {example-namespace} # eg: hubble-composer-service-account , this corresponds to k8s_service_account airflow variable

# Create google service account
gcloud iam service-accounts create {example-google-service-account} --project={gcp project name} # eg: hubble-composer-sa

# Give GCP bucket permissions to google service account. Also, you may want to do it for all projects, hubble , test-hubble to access all buckets

gcloud projects add-iam-policy-binding {gcp project name} --member="serviceAccount:{example-google-service-account}@{gcp project name}.iam.gserviceaccount.com" --role="roles/storage.admin"

# Attach docker secret created in step 6.a to k8s service account

kubectl patch serviceaccount {example-k8-service-account} -p '{"imagePullSecrets": [{"name": "google-docker-auth"}]}' --namespace={example-namespace}

# Associate the Google and k8s service accounts

gcloud iam service-accounts add-iam-policy-binding {example-google-service-account}@{gcp project name}.iam.gserviceaccount.com --role="roles/iam.workloadIdentityUser" --member="serviceAccount:{gcp project name}.svc.id.goog[{example-namespace}/{example-k8-service-account}]"

# Annotate the k8s service account with the Google service account

kubectl annotate serviceaccount {example-k8-service-account} --namespace {example-namespace} iam.gke.io/gcp-service-account={example-google-service-account}@{gcp project name}.iam.gserviceaccount.com
```

#### **c. Create clusterrolebinding**

There is a service account created automatically when we create a composer environment. We will be performing clusterrole binding on that service account.

To find the value of {composer_worker_namespace}, select your Cloud Composer environment, navigate to the ENVIRONMENT CONFIGURATION tab, and look for the value of GKE cluster. Click on the link that says view cluster workloads.
A new page will open with a list of Kubernetes workflows. Click on airflow-worker in order to go to the details page for that Deployment. Look for the value of Namespace.

To find the value of {composer_service_account}

```
kubectl create clusterrolebinding default-admin --clusterrole cluster-admin \
--serviceaccount={service_account_namespace} --namespace {example-namespace}

example: kubectl create clusterrolebinding default-admin --clusterrole cluster-admin --serviceaccount=composer-2-11-1-airflow-2-10-2-066f2865:default --namespace hubble-composer
```

To find the value of `<service_account_namespace>`, select your Cloud Composer environment, navigate to the `ENVIRONMENT CONFIGURATION` tab, and look for the value of `GKE cluster`. Click on the link that says `view cluster workloads`.

A new page will open with a list of Kubernetes workflows. Click on `airflow-worker` in order to go to the details page for that Deployment. Look for the value of `Namespace`.

### **Step 7. :tada: You should have successful Airflow Setup**

---

## **Airflow Variables Explanation**

### **Normal Variables**

| Variable name                 | Description                                                                                                                     | Should be changed?                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| api_key_path                  | Path to the Google Cloud Platform [API key](https://cloud.google.com/docs/authentication/api-keys?authuser=1)                   | No, unless your filename is different.                                |
| bq_dataset                    | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)                                                 | Yes. Change to your dataset name.                                     |
| bq_project                    | Name of the BigQuery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)               | Yes. Change to your project name.                                     |
| cluster_fields                | JSON object. Each key should be a BigQuery table, and the value is a list of columns that the table is clustered by             | Yes, if desired for tables that want clustering                       |
| gcs_exported_data_bucket_name | Name of the Google Cloud Storage [bucket](https://cloud.google.com/storage/docs/creating-buckets) that will store exported data | Yes. Change to the name of the bucket you made.                       |
| gcs_exported_object_prefix    | String to prefix run_id export task output path with                                                                            | Yes, if desired to prefix run_id                                      |
| image_name                    | Name of the ETL's Docker image                                                                                                  | No, unless you need a specific image version.                         |
| image_output_path             | Local output path within the ETL image                                                                                          | No.                                                                   |
| image_pull_policy             | Specifies how image pull behavior. Valid values are: `Always`, `IfNotPresent`, or `Never`                                       | No, unless you handle image updates manually.                         |
| local_output_path             | Local output path within the airflow-worker that is used for temporary storage                                                  | No, unless you changed the path when modifying the Kubernetes config. |
| namespace                     | Namespace name for ETL tasks that generate Kubernetes pods                                                                      | Yes, if you followed the optional step and made a new namespace       |
| output_file_names             | JSON object. Each key should be a data structure, and the value should be the name of the output file for that data structure   | Yes, if desired. Make sure each type has a different filename.        |
| output_path                   | Shared output path for exported data                                                                                            | No, unless you have a different shared storage solution.              |
| owner                         | The name of the owner of the Airflow DAGs                                                                                       | Yes.                                                                  |
| partition_fields              | JSON object. Each key should be a BigQuery table, and the value is a JSON object of type and field to partition by              | Yes, if desired for tables that want partitioning                     |
| public_dataset                | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)                                                 | Yes. Change to your dataset name.                                     |
| public_project                | Name of the BigQuery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)               | Yes. Change to your project name.                                     |
| sentry_dsn                    | Sentry Data Source Name to tell where Sentry SDK should send events                                                             | Yes                                                                   |
| sentry_environment            | Environment that sentry alerts will fire                                                                                        | Yes                                                                   |
| schema_filepath               | File path to schema folder                                                                                                      | No, unless schemas are in a different location                        |
| table_ids                     | JSON object. Each key should be a data structure, and the value should be the name of the BigQuery table                        | Yes, if desired. Make sure each type has a different table name.      |
| task_timeout                  | JSON object. Each key should be the airflow util task name, and the value is the timeout in seconds                             | Yes, if desired to give tasks timeout                                 |
| use_testnet                   | Flag to use testnet data instead of pubnet                                                                                      | Yes, if desired to use testnet data                                   |
| use_futurenet                 | Flag to use futurenet data instead of pubnet                                                                                    | Yes, if desired to use futurenet data                                 |
| use_captive_core              | Flag to use captive-core instead of the txmeta datastore                                                                        | Yes, if desired to run with captive-core                              |
| txmeta_datastore_path         | Bucket path to the txmeta datastore                                                                                             | Yes, change to your txmeta bucket path                                |

### **DBT Variables**

| Variable name                     | Description                                                                                                                              | Should be changed?                                        |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| dbt_full_refresh_models           | JSON object. Each key should be a DBT model, and the value is a boolean controlling if the model should be run with `--full-refresh`     | Yes, if desired for models that need to be full-refreshed |
| dbt_image_name                    | name of the `stellar-dbt` image to use                                                                                                   | No, unless you need a specific image version              |
| dbt_job_execution_timeout_seconds | timeout for dbt tasks in seconds                                                                                                         | No, unless you want a different timeout                   |
| dbt_job_retries                   | number of times dbt_jobs will retry                                                                                                      | No, unless you want a different retry limit               |
| dbt_mart_dataset                  | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets) for DBT marts                                            | Yes. Change to your dataset name                          |
| dbt_maximum_bytes_billed          | the max number of BigQuery bytes that can be billed when running DBT                                                                     | No, unless you want a different limit                     |
| dbt_project                       | name of the Biquery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)                         | Yes. Change to your project name                          |
| dbt_target                        | the `target` that will used to run dbt                                                                                                   | No, unless you want a different target                    |
| dbt_threads                       | the number of threads that dbt will spawn to build a model                                                                               | No, unless you want a different thread count              |
| dbt_tables                        | name of dbt tables to copy to sandbox                                                                                                    | No                                                        |
| dbt_internal_source_db            | Name of the BigQuery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)                        | Yes. Change to your project name.                         |
| dbt_internal_source_schema        | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)                                                          | Yes. Change to your dataset name.                         |
| dbt_public_source_db              | Name of the BigQuery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)                        | Yes. Change to your project name.                         |
| dbt_public_source_schema          | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)                                                          | Yes. Change to your dataset name.                         |
| dbt_slack_elementary_channel      | Name of slack channel to send elementary alerts                                                                                          | Yes. Change to your slack channel name.                   |
| dbt_elementary_dataset            | Name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)                                                          | Yes. Change to your dataset name.                         |
| dbt_elementary_secret             | Necessary argument for elementary task                                                                                                   | No                                                        |
| dbt_transient_errors_patterns     | Dictionary containing a name of a known dbt transient error as key and a list of string sentences to identify the error pattern as value | Yes, for every known error added                          |

### **Kubernetes-Specific Variables**

| Variable name            | Description                                                                                                                                                                                                                                                                                 | Should be changed?                                                                                     |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| resources                | Resources to request and allocate to Kubernetes Pods.                                                                                                                                                                                                                                       | No, unless pods need more resources                                                                    |
| kube_config_location     | Location of the kubernetes config file. See [here](https://www.astronomer.io/docs/cloud/stable/develop/kubepodoperator-local#get-your-kube-config) for a guide on finding the Kube config file. If you are running the pods in the same cluster as Airflow, you can leave this value blank. | No, unless the pods are in a different cluster than Airflow.                                           |
| kubernetes_sidecar_image | Image used for xcom sidecar                                                                                                                                                                                                                                                                 | No, unless you want to pull a different alpine-based image.                                            |
| k8s_namespace            | Namespace to run the task in                                                                                                                                                                                                                                                                | No, unless the pods are moved into a new namespace                                                     |
| k8s_service_account      | K8s service account the task runs as                                                                                                                                                                                                                                                        | No, unless k8s authentication is modified, and is likely linked to the associated GCP service account. |
| volume_config            | JSON objects representing the configuration for your Kubernetes volume.                                                                                                                                                                                                                     | Yes. Change configs to match your volume (see below for example configs)                               |
| volume_name              | Name of the persistent ReadWriteMany volume associated with the claim.                                                                                                                                                                                                                      | Yes. Change to your volume name.                                                                       |

Here are some example `volume_config` values. Note that a ReadWriteMany volume is required when tasks run in parallel.

- For a an NFS volume set `volume_config={"nfs": {"path": "/", "server": "my-server.provider.cloud"}}`.
- In order to set up a persistent volume claim, set `volume_config={"persistentVolumeClaim":{"claimName": <claim>}`
- In order to set up a host path volume, set `volume_config="hostPath":{"path": <path>, "type": "DirectoryOrCreate"}}`

<br>

---

# Execution Procedures

- [Starting Up](#starting-up)
- [Handling Failiures](#handling-failures)
  - [Clearing Failures](#clearing-failures)

## **Starting Up**

> **_NOTE:_** Google Cloud Composer instance of airflow has limited CLI support.
> [Supported Airflow CLI commands](https://cloud.google.com/composer/docs/composer-2/access-airflow-cli#supported-commands)

First, this [image](https://airflow.apache.org/docs/apache-airflow/2.7.3/ui.html#dags-view) shows the Airflow web UI components for pausing and triggering DAGs:

- Ensure that the Airflow scheduler is running: `airflow scheduler`
- Ensure that the Airflow web server is running: `airflow webserver -p <port>`
- Enable the DAGs
  - Use the command `airflow unpause <DAG name>` or use the Airflow UI

## **Handling Failures**

### **Clearing Failures**

You can clear failed tasks in the [grid-view](https://airflow.apache.org/docs/apache-airflow/2.7.3/ui.html#grid-view) in the Airflow UI. Clearing failed tasks gives them a chance to run again without requiring you to run the entire DAG again.

<br>

---

# Understanding the Setup

This section contains information about the Airflow setup. It includes our DAG diagrams and explanations of tasks. For general Airflow knowledge, check out the Airflow [concepts overview](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html) or the Airflow [tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html).

- [DAG Diagrams](#dag-diagrams)
  - [Public DAGs](#public-dags)
    - [History Table Export DAG](#history-table-export-dag)
    - [State Table Export DAG](#state-table-export-dag)
    - [DBT Enriched Base Tables DAG](#dbt-enriched-base-tables-dag)
  - [SDF Internal DAGs](#sdf-internal-dags)
    - [Sandbox DAGs](#sandbox-dags)
      - [Sandbox Create DAG](#sandbox-create-dag)
      - [Sandbox Update DAG](#sandbox-update-dag)
    - [Cleanup Metadata DAG](#cleanup-metadata-dag)
    - [Partner Pipeline DAG](#partner-pipeline-dag)
    - [DBT SDF Marts DAG](#dbt-sdf-marts-dag)
    - [Daily Euro OHLC DAG](#daily-euro-ohlc-dag)
    - [Audit Log DAG](#audit-log-dag)
- [Task Explanations](#task-explanations)
  - [build_time_task](#build_time_task)
  - [build_export_task](#build_export_task)
  - [build_gcs_to_bq_task](#build_gcs_to_bq_task)
  - [build_apply_gcs_changes_to_bq_task](#build_apply_gcs_changes_to_bq_task)
  - [build_del_ins_from_gcs_to_bq_task](#build_del_ins_from_gcs_to_bq_task)
  - [build_batch_stats](#build_batch_stats)
  - [bq_insert_job_task](#bq_insert_job_task)
  - [cross_dependency_task](#cross_dependency_task)
  - [build_delete_data_task](#build_delete_data_task)
  - [build_dbt_task](#build_dbt_task)
  - [build_elementary_slack_alert_task](#build_elementary_slack_alert_task)

---

## **DAG Diagrams**

- The sources are: ledgers, operations, transactions, trades, effects, claimable_balances, accounts, account_signers, liquidity_pools, offers, trust_lines, config_settings, contract_data, contract_code and assets.

- The DAGs that export the sources are: History Table Export and State Table Export.

- All the other tables that are not listed above are exclusive to internal datasets.

### **Public DAGs**

#### **History Table Export DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/history_tables_dag.py):

- Exports part of sources: ledgers, operations, transactions, trades, effects and assets from Stellar using CaptiveCore
- Inserts into BigQuery publicly (crypto-stellar).

![history_table_export DAG](documentation/images/history_table_export.png)

#### **State Table Export DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/state_table_dag.py)

- Exports accounts, account_signers, offers, claimable_balances, liquidity pools, trustlines, contract_data, contract_code, config_settings and ttl.
- Inserts into BigQuery publicly (crypto stellar).

![state_table_export DAG](documentation/images/state_table_export.png)

#### **DBT Enriched Base Tables DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/dbt_enriched_base_tables_dag.py)

- Creates the DBT staging views for models
- Updates the enriched_history_operations table
- Updates the current state tables
- If found any warnings, it sends a Slack notification about what table has a warning, the time and date it ocurred.

![dbt_enriched_base_tables DAG](documentation/images/dbt_enriched_base_tables.png)

### **SDF Internal DAGs**

#### **Sandbox DAGs**

- The sandbox dags are used to provide Canvas with a subset of network data that will fit within their systems
- The tables contain 6 months of the targeted environment's (pubnet, testnet, or futurenet) data.

##### **Sandbox Create DAG**

- This DAG runs only once and creates the Canvas sandbox dataset with copies of transactions tables, state tables, and current state views.

##### **Sandbox Update DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/sandbox_update_dag.py)

- This DAG update the Canvas sandbox dataset with transactions tables and state tables with history once a daily.

![sandbox_update_dag DAG](documentation/images/sandbox_update_dag.png)

#### **Cleanup Metadata DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/cleanup_metadata_dag.py)

- A maintenance workflow that you can deploy into Airflow to periodically clean
  out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
  having too much data in your Airflow MetaStore.

![cleanup_metadata_dag DAG](documentation/images/cleanup_metadata_dag.png)

#### **Partner Pipeline DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/partner_pipeline_dag.py)

- Used by SDF for internal partnership pipelines

#### **DBT SDF Marts DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/dbt_sdf_marts_dag.py)

- Updates the DBT mart tables daily
- If found any warnings, it sends a Slack notification about what table has a warning, the time and date it ocurred.

![dbt_sdf_marts DAG](documentation/images/dbt_sdf_marts.png)

#### **Daily Euro OHLC DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/daily_euro_ohlc_dag.py)

- Updates the currency table in Bigquey for Euro

![daily_euro_ohlc_dag DAG](documentation/images/daily_euro_ohlc_dag.png)

#### **Audit Log DAG**

[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/audit_log_dag.py)

- Runs an audit log SQL to update the audit log dashboard

![audit_log_dag DAG](documentation/images/audit_log_dag.png)

<br>

---

## **Task Explanations**

### **build_time_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_time_task.py) contains methods for creating time tasks. Time tasks call the get_ledger_range_from_times function in the stellar-etl Docker image. The tasks receive the execution time of the current DAG run and the expected execution time of the next run. They convert this time range into a ledger range that can be passed to the export tasks.

### **build_export_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_export_task.py) contains methods for creating export tasks. Export tasks call export functions in the stellar-etl Docker image with a ledger range determined by the upstream time task. The data is exported in a newline-delimited JSON text file with a file name in the format `[start ledger]-[end ledger]-[data type].txt`.

### **build_gcs_to_bq_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_gcs_to_bq_task.py) contains methods for creating tasks that appends information from a Google Cloud Storage file to a BigQuery table. These tasks will create a new table if one does not exist. These tasks are used for history archive data structures, as Stellar wants to keep a complete record of the ledger's entire history.

### **build_del_ins_from_gcs_to_bq_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_del_ins_from_gcs_to_bq_task.py) contains methods for deleting data from a specified BigQuery table according to the batch interval and also imports data from gcs to the corresponding BigQuery table. These tasks will create a new table if one does not exist. These tasks are used for history and state data structures, as Stellar wants to keep a complete record of the ledger's entire history.

### **build_apply_gcs_changes_to_bq_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_apply_gcs_changes_to_bq_task.py) contains methods for creating apply tasks. Apply tasks are used to merge a file from Google Cloud Storage into a BigQuery table. Apply tasks differ from the other task that appends in that they apply changes. This means that they update, delete, and insert rows. These tasks are used for accounts, offers, and trustlines, as the BigQuery table represents the point in time state of these data structures. This means that, for example, a merge task could alter the account balance field in the table if a user performed a transaction, delete a row in the table if a user deleted their account, or add a new row if a new account was created.

Apply tasks can also be used to insert unique values only. This behavior is used for orderbook and history archive data structures. Instead of performing a merge operation, which would update or delete existing rows, the task will simply insert new rows if they don't already exist. This helps prevent duplicated data in a scenario where rows shouldn't change or be deleted. Essentially, this task replicates the behavior of a primary key in a database when used for orderbooks.

### **build_batch_stats**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_batch_stats.py) pulls and inserts batch stats into BigQuery.
Data is inserted into `history_archives_dag_runs`.

### **bq_insert_job_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_bq_insert_job_task.py) contains methods for creating BigQuery insert job tasks.
The task will read the query from the specified sql file and will return a BigQuery job operator configured to the GCP project and datasets defined.

### **cross_dependency_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_cross_dependency_task.py) creates an ExternalTaskSensor that triggers on specified DAG tasks's success.

### **build_delete_data_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_delete_data_task.py) deletes data from a specified BigQuery `project.dataset.table` according to the batch interval.

> _*NOTE:*_ If the batch interval is changed, the deleted data might not align with the prior batch intervals.

### **build_copy_table_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_copy_table_task.py) copies a table from an specified BigQuery `project.dataset.table` to its destination inside of an specific project and dataset.

### **build_coingecko_api_to_gcs_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_coingecko_api_to_gcs_task.py) creates a CSV file from the CoinGecko API and uploads it to Google Cloud Storage, inside of a bucket.
The file is named after the destination_blob_name parameter and the columns parameter is used to create the CSV file with the specified columns.

### **build_check_execution_date_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_check_execution_date_task.py) checks if the current date is in the list of dates that are set to reset the DAG.
If the current date is in the list, the DAG will continue to run the tasks that are set to run on the reset date. If the current date is not in the list, the DAG will stop running the tasks that are set to run on the reset date.

### **build_dbt_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_dbt_task.py) contains methods for creating dbt tasks. DBT tasks run `dbt build` for the specified model.

### **build_elementary_slack_alert_task**

[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_elementary_slack_alert_task.py) contains methods for enabling the elementary slack alerts.

<br>

---

# Further Development

This section details further areas of development. It covers a basic guide on how to add new features and test changes to existing features. It also contains a list of project TODOs (check the GitHub [issues page](https://github.com/stellar/stellar-etl-airflow/issues) for more!)

---

## **Extensions**

This section covers some possible extensions or further work that can be done.

- [Pre-commit Git hook scripts](#pre-commit-git-hook-scripts)
- [Adding New DAGs](#adding-new-dags)
- [Adding tasks to existing DAGs](#adding-tasks-to-existing-dags)
- [Adding New Tasks](#adding-new-tasks)

<br>

### **Pre-commit Git hook scripts**

Git can run special scripts at various places in the Git workflow (which the system calls “hooks”).
These scripts can do whatever you want and, in theory, can help a team with their development flow.

`pre-commit` makes hook scripts extremely accessible to teams.

- Install `pre-commit`

  ```bash
  # using pip
  $ pip install pre-commit==3.2.1
  ```

- Set up the Git hook scripts

  ```bash
  $ pre-commit install
  pre-commit installed at .git/hooks/pre-commit
  ```

That's it. Now `pre-commit` will run automatically on `git commit`!

<br>

### **Adding New DAGs**

Adding new DAGs is a fairly straightforward process. Create a new python file in the `dags` folder. Create your dag object using the code below:

```
dag = DAG(
	'dag_id',
	default_args=get_default_dag_args(),
	description='DAG description.',
	schedule_interval=None,
)
```

The `get_default_dag_args()` is defined in the [dags/stellar-etl-airflow/default.py](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/default.py) file.

Feel free to add more arguments or customize the existing ones. The documentation for a DAG is available [here](https://airflow.apache.org/docs/stable/_api/airflow/models/dag/index.html).

<br>

### **Adding tasks to existing DAGs**

If you have created a new DAG, or wish to extend an existing DAG, you can add tasks to it by calling the various `create_X_task` functions that are in the repository. See [here](https://airflow.apache.org/docs/stable/concepts.html#relations-between-tasks) for details on how to create dependencies between tasks.

<br>

### **Adding New Tasks**

Adding new tasks is a more involved process. You likely need to add a new python file in the `dags/stellar_etl_airflow` folder. This file should include a function that creates and returns the new task, as well as any auxiliary functions related to the task.

Airflow has a variety of operators. The ones that are most likely to be used are:

- [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html#airflow.providers.docker.operators.docker.DockerOperator), which can be used to execute commands within a docker container
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html), which can start new Kubernetes Pods
- [PythonOperator](https://airflow.apache.org/docs/stable/howto/operator/python.html), which can run Python functions

You may also find this list of [Google-related operators](https://airflow.apache.org/docs/stable/howto/operator/gcp/index.html) useful for interacting with Google Cloud Storage or BigQuery.

An example of a simple task is the [time task](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_time_task.py). This task converts a time into a ledger range using a stellar-etl command. Since it needs to use the stellar-etl, we need a KubernetesPodOperator. We provide the operator with the command, the task_id, the parent DAG, and some parameters specific to KubernetesPodOperator.

More complex tasks might require a good amount of extra code to set up variables, authenticate, or check for errors. However, keep in mind that tasks should be idempotent. This means that tasks should produce the same output even if they are run multiple times. The same input should always produce the same output.

You may find that you need to pass small amounts of information, like filenames or numbers, from one task to another. You can do so with Airflow's [XCOM system](https://airflow.apache.org/docs/stable/concepts.html?highlight=xcom#xcoms).

<br>

## **Testing Changes**

Once you make a change, you can test it using the Airflow command line interface. Here's a quick outline of how to test changes:

- Run `kubectl get pods --all-namespaces`. Look for a pod that starts with `airflow-worker`.
- Run `kubectl -n <pod_namespace> exec -it airflow-worker-<rest_of_pod_name> -c airflow-worker -- /bin/bash` to get inside the worker
- Run `airflow task test history_archive_export <task_id> <test_date>`. Note that if the task you changed has dependencies, you need to run `airflow test` on those upstream tasks for the exact same date.
- Run `airflow task test` on the tasks that depend on the the task you just changed. Ensure that they still perform as expected.

This guide can also be useful for testing deployment in a new environment. Follow this testing process for all the taks in your DAGs to ensure that they work end-to-end.

An alternative to the testing flow above is to `trigger` the task in the Airflow UI. From there you are able to view the task status, log, and task details.
