'''
This file contains functions for creating Airflow tasks to convert from a time range to a ledger range.
'''

from stellar_etl_airflow.docker_operator import DockerOperator  
from airflow.models import Variable

def build_time_task(dag, use_next_exec_time=True):
    '''
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl Docker image. The start time is the execution time.
    When use_next_exec_time is True, the end time is the next execution time. If it is False, then the end time is the same
    as the start time. The task that is returned allows for retreiving the ledger range for the given execution. The range object
    is sent as a string representation of a JSON object to the xcom, where it can be accessed by subsequent tasks.
    
    Parameters:
        dag - parent dag that the task will be attached to 
        use_next_exec_time - determines whether to use the next execution time or replace it with the current execution time
    Returns:
        the newly created task
    '''

    end_time = '{{ next_execution_date.isoformat() }}' if use_next_exec_time else '{{ ts }}'
    command = "stellar-etl get_ledger_range_from_times -s {{ ts }} --stdout -e " + end_time
    return DockerOperator(
        task_id='get_ledger_range_from_times',
        image=Variable.get('image_name'),
        command=command, 
        volumes=[f'{Variable.get("local_output_path")}:{Variable.get("image_output_path")}'],
        dag=dag,
        xcom_push=True,
        auto_remove=True,
    )
