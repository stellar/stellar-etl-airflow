from datetime import timedelta
import logging
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from kubernetes.client import models as k8s
from stellar_etl_airflow.default import alert_after_max_retries

def create_dbt_profile():
    dbt_target = Variable.get('dbt_target')
    dbt_dataset = Variable.get('dbt_dataset')
    dbt_maximum_bytes_billed = Variable.get('dbt_maximum_bytes_billed')
    dbt_job_execution_timeout_seconds = Variable.get('dbt_job_execution_timeout_seconds')
    dbt_job_retries = Variable.get('dbt_job_retries')
    dbt_project = Variable.get('dbt_project')
    dbt_threads = Variable.get('dbt_threads')
    dbt_private_key_id = Variable.get('dbt_private_key_id')
    dbt_private_key = Variable.get('dbt_private_key')
    dbt_client_email = Variable.get('dbt_client_email')
    dbt_client_id = Variable.get('dbt_client_id')
    dbt_auth_uri = Variable.get('dbt_auth_uri')
    dbt_token_uri = Variable.get('dbt_token_uri')
    dbt_auth_provider_x509_cert_url = Variable.get('dbt_auth_provider_x509_cert_url')
    dbt_client_x509_cert_url = Variable.get('dbt_client_x509_cert_url')

    profiles_yml = f"""
stellar_dbt:
  target: {dbt_target}
  outputs:
    {dbt_target}:
      dataset: {dbt_dataset}
      maximum_bytes_billed: {dbt_maximum_bytes_billed}
      job_execution_timeout_seconds: {dbt_job_execution_timeout_seconds}
      job_retries: {dbt_job_retries}
      location: us
      method: service-account-json
      project: "{dbt_project}"
      threads: {dbt_threads}
      type: bigquery
      keyfile_json:
        type: "service_account"
        project_id: "{dbt_project}"
        private_key_id: "{dbt_private_key_id}"
        private_key: "{dbt_private_key}"
        client_email: "{dbt_client_email}"
        client_id: "{dbt_client_id}"
        auth_uri: "{dbt_auth_uri}"
        token_uri: "{dbt_token_uri}"
        auth_provider_x509_cert_url: "{dbt_auth_provider_x509_cert_url}"
        client_x509_cert_url: "{dbt_client_x509_cert_url}"
"""

    create_dbt_profile_cmd = f"echo '{profiles_yml}' > profiles.yml;"

    return create_dbt_profile_cmd

def build_dbt_task(dag, model_name, resource_cfg="default"):
    """ Create a task to run dbt on a selected model.

    args:
        dag: parent dag for this task
        model_name: dbt model_name to run
        resource_cfg: the resource config name defined in the airflow 'resources' variable for k8s

    returns:
        k8s pod task
    """

    dbt_full_refresh = ""
    dbt_full_refresh_models = Variable.get('dbt_full_refresh_models', deserialize_json=True)
    if dbt_full_refresh_models.get(model_name):
        dbt_full_refresh = "--full-refresh"

    create_dbt_profile_cmd = create_dbt_profile()

    command = ["sh", "-c"]
    args = [" ".join([create_dbt_profile_cmd, "dbt run --select", model_name, dbt_full_refresh])]
    logging.info(f'sh commands to run in pod: {args}')

    config_file_location = Variable.get('kube_config_location')
    in_cluster = False if config_file_location else True
    resources_requests = Variable.get('resources', deserialize_json=True).get(resource_cfg).get('requests')
    affinity = Variable.get('affinity', deserialize_json=True).get(resource_cfg)

    return KubernetesPodOperator(
         task_id=model_name,
         name=model_name,
         execution_timeout=timedelta(seconds=Variable.get('task_timeout', deserialize_json=True)[build_dbt_task.__name__]),
         namespace=Variable.get('k8s_namespace'),
         service_account_name=Variable.get('k8s_service_account'),
         image=Variable.get('dbt_image_name'),
         cmds=command,
         arguments=args,
         dag=dag,
         do_xcom_push=True,
         is_delete_operator_pod=True,
         startup_timeout_seconds=720,
         in_cluster=in_cluster,
         config_file=config_file_location,
         affinity=affinity,
         container_resources=k8s.V1ResourceRequirements(requests=resources_requests),
         on_failure_callback=alert_after_max_retries,
         image_pull_policy='Always' # TODO: Update to ifNotPresent when image pull issue is fixed
     ) 
