from airflow.sensors.external_task import ExternalTaskSensor


def build_cross_deps(dag, task, parent_dag, parent_task=None):

    return ExternalTaskSensor(
        task_id=f"check_{task}_finish",
        external_dag_id=parent_dag,
        external_task_id=parent_task, # None means wait for the entire DAG to finish
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed'],
        mode="reschedule",
    )