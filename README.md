# stellar-etl-airflow
This repository contains the Airflow DAGs for the [Stellar ETL](https://github.com/stellar/stellar-etl) project. These DAGs provide a workflow for exporting data from the Stellar network and uploading the data into BigQuery.

## Table of Contents
- [Stellar ETL Airflow](#stellar-etl-airflow)
  - [Installation and Setup](#installation-and-setup)
  - [Task Explanations](#task-explanations)
  - [DAG Diagrams](#dag-diagrams)
  - [Execution Procedures](#execution-procedures)
  - [Remaining Project TODOs](#remaining-project-todos)

## Installation and Setup
1. Install Airflow v1.10 or later: `pip install apache-airflow`
    - To confirm Airflow is installed, run `airflow -h` and ensure that you see a help screen
2. Install the required packages: `pip install -r requirements.txt` 
3. Setup the Airflow database: `airflow initdb`
4. Run Airflow scheduler: `airflow scheduler`
5. Run Airflow web server: `airflow webserver`
6. Add required Airflow variables through [CLI](https://airflow.apache.org/docs/stable/cli-ref#variables) or the [Airflow UI](https://airflow.apache.org/docs/stable/ui.html#variable-view)
    - api_key_path: path to the Google Cloud Platform [API key](https://cloud.google.com/docs/authentication/api-keys?authuser=1)
    - bq_dataset: name of the BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets)
    - bq_project: name of the BigQuery [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#console)
    - core_cfg_path: path to the [stellar core configuration](https://www.stellar.org/developers/stellar-core/software/admin.html#configuring)
    - core_exec_path: filepath to the [stellar core executable](https://github.com/stellar/stellar-core/blob/master/INSTALL.md)
    - gcs_bucket_name: name of the Google Cloud Storage [bucket](https://cloud.google.com/storage/docs/creating-buckets) that will store exported data
    - image_name: name of the ETL's Docker image
    - image_output_path: local output path within the ETL image
    - output_file_names: JSON object. Each key should be a data structure, and the value should be the name of the output file for that data structure
    - output_path: local output path
    - owner: the name of the owner of the Airflow DAGs
    - table_ids: JSON object. Each key should be a data structure, and the value should be the name of the BigQuery table
7. Add required Airflow connections through the [CLI](https://airflow.apache.org/docs/stable/cli-ref#connections) or [Airflow UI](https://airflow.apache.org/docs/stable/howto/connection/index.html) 
    - google_cloud_platform_connection: connection of type google_cloud_platform that connects to a Google Cloud Platform API key for a specific project. See [here](https://cloud.google.com/docs/authentication/api-keys?authuser=1) for more information about API keys.
    - fs_default: connection with fs type that sets the default filepath

Alternatively, a Docker image for the Airflow setup will be available soon.

## DAG Diagrams
### History Archive Export DAG
[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/history_archive_dag.py) exports ledgers, transactions, operations, and trades from Stellar's history archives, loads them into Google Cloud Storage, and then sends the data to BigQuery.
![History Archive Dag](https://i.ibb.co/jTrkfg3/History-Archive-DAG.png)

### Unbounded Core Export DAG
[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/unbounded_core_dag.py) connects to a stellar-core instance and exports accounts, offers, and trustlines. This DAG is a long-running process that continually exports new information as the Stellar network progresses.
![Core DAG](https://i.ibb.co/BPyvt6M/Core-DAG.png)
### Bounded Core Export DAG
[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/bounded_core_dag.py) connects to a stellar-core instance and exports accounts, offers, and trustlines. Unlike the unbounded version, this version is not long running. It stops once the range has been exported. Currently, this range is the ledger that includes the DAG's execution date.
![Core DAG](https://i.ibb.co/BPyvt6M/Core-DAG.png)
### Process Unbounded Core DAG
[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/process_unbounded_core_dag.py) processes the output of the unbounded core DAG. File sensors watch the folder where the unbounded core DAG sends its exported information. Once a file is seen, it is loaded into Google Cloud Storage and applied to BigQuery. Once a batch has been exported completely, the DAG triggers itself again.
![Process Core DAG](https://i.ibb.co/0cb8C8V/Process-DAG.png)
### Bucket List Export DAG
[This DAG](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/bucket_list_dag.py) exports from Stellar's bucket list, which contains data on accounts, offers, and trustlines. Exports from this DAG always begins from the genesis ledger and end at the ledger that includes the DAG's execution date.
![Bucket List DAG](https://i.ibb.co/RPM21dS/Bucket-List-DAG.png)

## Execution Procedures
1. Ensure that the Airflow scheduler is running: `airflow scheduler`
2. Enable the History Archive Export DAG
    - Use the command `airflow unpause history_archive_export`
    - The DAG will export information every 5 minutes. It also will backfill by exporting information starting at the network's beginning up until the current time
3. Enable the Unbounded Export and Process Unbounded DAGs
    - Use the commands `airflow unpause unbounded_core_export` and `airflow unpause process_unbounded_core`
    - Manually trigger each of these DAGs to get them started with `airflow trigger_dag unbounded_core_export` and `airflow trigger_dag process_unbounded_core`

## Task Explanations
### build_time_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_time_task.py) contains methods for creating time tasks. Time tasks call the get_ledger_range_from_times function in the stellar-etl Docker image. The tasks receive the execution time of the current DAG run and the expected execution time of the next run. They convert this time range into a ledger range that can be passed to the export tasks.

### build_export_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_export_task.py) contains methods for creating export tasks. Export tasks call export functions in the stellar-etl Docker image with a ledger range determined by the upstream time task. The data is exported in a newline-delimited JSON text file with a file name in the format `[start ledger]-[end ledger]-[data type].txt`.

### build_load_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_load_task.py) contains methods for creating load tasks. Load tasks load local files that were exported into Google Cloud Storage. In order to keep GCS files organized, exported data is loaded into a folder called `exported`. The `exported` folder contains folders for each of the exported data types.

### build_gcs_to_bq_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_gcs_to_bq_task.py) contains methods for creating tasks that appends information from a Google Cloud Storage file to a BigQuery table. These tasks will create a new table if one does not exist. These tasks are used for history archive data structures, as Stellar wants to keep a complete record of the ledger's entire history.

### build_file_sensor_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_file_sensor_task.py) contains methods for creating a file sensor task. File sensors take in a file path, and continuously check that file path until a file or folder exists. Once the file is sensed, the task succeeds. This task is important because the unbounded core DAG exports batches at variable times. Using file sensors ensures that batches are detected and processed as soon as they are exported.

### build_apply_gcs_changes_to_bq_task
[This file](https://github.com/stellar/stellar-etl-airflow/blob/master/dags/stellar_etl_airflow/build_apply_gcs_changes_to_bq_task.py) contains methods for creating apply tasks. Apply tasks are used to merge a file from Google Cloud Storage into a BigQuery table. Apply tasks differ from the other task that appends in that they apply changes. This means that they update, delete, and insert rows. These tasks are used for accounts, offers, and trustlines, as the BigQuery table represents the point in time state of these data structures. This means that, for example, a merge task could alter the account balance field in the table if a user performed a transaction, delete a row in the table if a user deleted their account, or add a new row if a new account was created.


## Remaining Project TODOs
- The Docker image for the ETL and Airflow need to be uploaded to Docker Hub
- More detailed logging in DAGs ([#4](https://github.com/stellar/stellar-etl-airflow/issues/4))
- Set up alerting for task failures ([#5](https://github.com/stellar/stellar-etl-airflow/issues/5))
- Set up spend monitoring for BigQuery operations and Google Cloud Storage space usage ([#6](https://github.com/stellar/stellar-etl-airflow/issues/6))
