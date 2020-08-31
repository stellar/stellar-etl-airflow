'''
This file contains functions for creating Airflow tasks to convert from a time range to a ledger range.
'''

from airflow.operators.bash_operator import BashOperator

def build_date_task(dag):
    '''
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl. The start time is the execution time,
    and the end time is the next execution time. Allows for retreiving the ledger range for the given execution. The range object
    sent as a string representation of a JSON object to the xcom, where it can be accessed by subsequent tasks.
    
    Parameters:
        dag - parent dag that the task will be attached to 
    Returns:
        the newly created task
    '''

    return BashOperator(
        task_id='get_ledger_range_from_times',
        bash_command='stellar-etl get_ledger_range_from_times -s {{ ts }} -e {{ next_execution_date.isoformat() }} --stdout',
        dag=dag,
        xcom_push=True,
    )