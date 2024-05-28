"""
This file contains functions for creating Airflow tasks to convert from a time range to a ledger range.
"""
import logging
from datetime import timedelta

from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from stellar_etl_airflow.default import alert_after_max_retries


def build_time_task(
    dag,
    use_gcs=True,
    use_testnet=True,
    use_next_exec_time=True,
    resource_cfg="default",
    use_futurenet=False,
):
    """
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl Docker image. The start time is the previous
    execution time. Since checkpoints are only written to History Archives every 64 ledgers, we have to account for a 5-6 min delay.
    When use_next_exec_time is True, the end time is the current execution time. If it is False, then the end time is the same
    as the start time. The task that is returned allows for retreiving the ledger range for the given execution. The range object
    is sent as a string representation of a JSON object to the xcom, where it can be accessed by subsequent tasks.

    Parameters:
        dag - parent dag that the task will be attached to
        use_next_exec_time - determines whether to use the next execution time or replace it with the current execution time
    Returns:
        the newly created task
    """
    start_time = "{{ subtract_data_interval(dag, data_interval_start).isoformat() }}"
    end_time = (
        "{{ subtract_data_interval(dag, data_interval_end).isoformat() }}"
        if use_next_exec_time
        else "{{ ts }}"
    )
    start_time = "2024-05-27T17:10:00+00:00"
    end_time = "2024-05-27T17:20:00+00:00"

    command = ["stellar-etl"]
    # Inclui no stellar etl get ledger ranges from times em vez de output mandar para o GCS bucket
    # Depois refatorar no airflow como pegar o ledgers ranges para construir novamente o generate_etl_command
    args = [
        "get_ledger_range_from_times",
        "-s",
        start_time,
        "-o",
        "us-central1-test-hubble-2-5f1f2dbf-bucket/dag-exported/scheduled__2024-05-27T17:20:00+00:00/ledgers_range.txt",
        "-e",
        end_time,
    ]
    logging.info(f"Constructing command with args: {args}")
    if use_testnet:
        args.append("--testnet")
    elif use_futurenet:
        args.append("--futurenet")
    elif use_gcs:
        args.extend(
            ["--cloud-storage-bucket", Variable.get("gcs_exported_data_bucket_name")]
        )
        args.extend(["--cloud-provider", "gcp"])
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

    return KubernetesPodOperator(
        task_id="get_ledger_range_from_times",
        name="get_ledger_range_from_times",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_time_task.__name__
            ]
        ),
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        image="{{ var.value.image_name }}",
        cmds=command,
        arguments=args,
        dag=dag,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        affinity=affinity,
        container_resources=resources_requests,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy=Variable.get("image_pull_policy"),
    )
