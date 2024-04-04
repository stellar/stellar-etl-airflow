"""
This file contains functions for creating Airflow tasks to run stellar-etl export functions.
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import AirflowException
from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def get_path_variables(use_testnet=False, use_futurenet=False):
    """
    Returns the image output path, core executable path, and core config path.
    """
    config = "/etl/docker/stellar-core.cfg"
    if use_testnet:
        config = "/etl/docker/stellar-core_testnet.cfg"
    elif use_futurenet:
        config = "/etl/docker/stellar-core_futurenet.cfg"
    return "/usr/bin/stellar-core", config


def select_correct_filename(cmd_type, base_name, batched_name):
    switch = {
        "archive": batched_name,
        "bucket": batched_name,
        "bounded-core": base_name,
        "unbounded-core": base_name,
    }
    filename = switch.get(cmd_type, "No file")
    if filename == "No file":
        raise AirflowException("Command type is not supported: ", cmd_type)
    return filename


def generate_etl_cmd(
    command,
    base_filename,
    cmd_type,
    use_gcs=False,
    use_testnet=False,
    use_futurenet=False,
    use_captive_core=False,
    txmeta_datastore_url="",
):
    """
    Runs the provided stellar-etl command with arguments that are appropriate for the command type.
    The supported command types are:
        'archive' - indicates that information is being read within a bounded range from the history archives
        'bucket' - indicates that information is being read up to an end time from the history archives' bucket list.
        'bounded-core' - indicates that information is being read within a bounded range from stellar-core
        'unbounded-core' - indicates that information is being read from the start time onwards from stellar-core

    The supported commands are export_accounts, export_ledgers, export_offers, export_operations, export_trades, export_transactions, and export_trustlines.

    Parameters:
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        base_filename - base filename for the output file or folder; the ledger range is pre-pended to this filename
        cmd_type - the type of the command, which is determined by the information source
    Returns:
        the generated etl command; name of the file that contains the exported data
    """
    # These are JINJA templates, which are filled by airflow at runtime. The json from get_ledger_range_from_times is pulled from XCOM.
    logging.info("Pulling ledger start and end times.....")
    start_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"] }}'
    end_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"]}}'

    """
    For history archives, the start time of the next 5 minute interval is the same as the end time of the current interval, leading to a 1 ledger overlap.
    We need to subtract 1 ledger from the end so that there is no overlap. However, sometimes the start ledger equals the end ledger.
    By setting the end=max(start, end-1), we ensure every range is valid.
    """
    if cmd_type in ("archive", "bounded-core"):
        end_ledger = '{{ [ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"]-1, ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"]] | max}}'

    core_exec, core_cfg = get_path_variables(use_testnet, use_futurenet)

    batch_filename = "-".join([start_ledger, end_ledger, base_filename])
    run_id = "{{ run_id }}"
    filepath = ""
    if use_gcs:
        filepath = os.path.join(Variable.get("gcs_exported_object_prefix"), run_id)
    batched_path = os.path.join(filepath, batch_filename)
    base_path = os.path.join(filepath, base_filename)
    if command == "export_all_history":
        batched_path = filepath + "/"
    correct_filename = select_correct_filename(cmd_type, base_filename, batch_filename)
    switch = {
        "archive": [
            "stellar-etl",
            command,
            "-s",
            start_ledger,
            "-e",
            end_ledger,
            "-o",
            batched_path,
        ],
        "bucket": ["stellar-etl", command, "-e", end_ledger, "-o", batched_path],
        "bounded-core": [
            "stellar-etl",
            command,
            "-s",
            start_ledger,
            "-e",
            end_ledger,
            "-x",
            core_exec,
            "-c",
            core_cfg,
            "-o",
            base_path,
        ],
        "unbounded-core": [
            "stellar-etl",
            command,
            "-s",
            start_ledger,
            "-x",
            core_exec,
            "-c",
            core_cfg,
            "-o",
            base_path,
        ],
    }

    cmd = switch.get(cmd_type, None)
    if cmd is None:
        raise AirflowException("Command type is not supported: ", cmd_type)
    if use_gcs:
        cmd.extend(
            ["--cloud-storage-bucket", Variable.get("gcs_exported_data_bucket_name")]
        )
        # TODO: cloud-provider should be a parameter instead of hardcoded to gcp
        cmd.extend(["--cloud-provider", "gcp"])
        batch_id = macros.get_batch_id()
        batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"
        batch_insert_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        metadata = f"'batch_id={batch_id},batch_run_date={batch_date},batch_insert_ts={batch_insert_ts}'"
        cmd.extend(["-u", metadata])
    if use_testnet:
        cmd.append("--testnet")
    elif use_futurenet:
        cmd.append("--futurenet")
    if use_captive_core:
        cmd.append("--captive-core")
    if txmeta_datastore_url:
        cmd.extend(["--datastore-url", txmeta_datastore_url])
    cmd.append("--strict-export")

    if command == "export_all_history":
        return cmd, filepath + "/"

    return cmd, os.path.join(filepath, correct_filename)


def build_export_task(
    dag,
    cmd_type,
    command,
    filename,
    use_gcs=False,
    use_testnet=False,
    use_futurenet=False,
    resource_cfg="default",
    use_captive_core=False,
    txmeta_datastore_url="",
):
    """
    Creates a task that calls the provided export function with the correct arguments in the stellar-etl Docker image.
    Runs in a KubernetesPodOperator.

    Parameters:
        dag - the parent dag
        cmd_type - the type of the command, which is determined by the information source
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        filename - filename for the output file or folder
    Returns:
        the newly created task
    """

    etl_cmd, output_file = generate_etl_cmd(
        command,
        filename,
        cmd_type,
        use_gcs,
        use_testnet,
        use_futurenet,
        use_captive_core,
        txmeta_datastore_url,
    )
    etl_cmd_string = " ".join(etl_cmd)
    namespace = conf.get("kubernetes", "NAMESPACE")
    if namespace == "default":
        config_file_location = Variable.get("kube_config_location")
        in_cluster = False
    else:
        config_file_location = None
        in_cluster = True
    resources_requests = (
        f"{{{{ var.json.resources.{resource_cfg}.requests | container_resources }}}}"
    )
    affinity = Variable.get("affinity", deserialize_json=True).get(resource_cfg)
    if command == "export_ledger_entry_changes" or command == "export_all_history":
        arguments = f"""{etl_cmd_string} && echo "{{\\"output\\": \\"{output_file}\\"}}" >> /airflow/xcom/return.json"""
    else:
        arguments = f"""
                    {etl_cmd_string} 2>> stderr.out && cat stderr.out && echo "{{\\"output\\": \\"{output_file}\\",
                    \\"failed_transforms\\": `grep failed_transforms stderr.out | cut -d\\":\\" -f2`,
                    \\"successful_transforms\\": `grep successful_transforms stderr.out | cut -d\\":\\" -f3 | cut -d\\",\\" -f2`}}" >> /airflow/xcom/return.json
                    """
    return KubernetesPodOperator(
        service_account_name=Variable.get("k8s_service_account"),
        namespace=Variable.get("k8s_namespace"),
        task_id=command + "_task",
        execution_timeout=timedelta(
            minutes=Variable.get("task_timeout", deserialize_json=True)[
                build_export_task.__name__
            ]
        ),
        name=command + "_task",
        image="{{ var.value.image_name }}",
        cmds=["bash", "-c"],
        arguments=[arguments],
        dag=dag,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        container_resources=resources_requests,
        in_cluster=in_cluster,
        config_file=config_file_location,
        affinity=affinity,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy=Variable.get("image_pull_policy"),
    )
