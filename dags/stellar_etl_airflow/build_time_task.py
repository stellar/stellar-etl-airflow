'''
This file contains functions for creating Airflow tasks to convert from a time range to a ledger range.
'''

from airflow.operators.bash_operator import BashOperator

def build_time_task(dag, use_next_exec_time=True):
    '''
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl. The start time is the execution time.
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
    return BashOperator(
        task_id='get_ledger_range_from_times',
        bash_command='stellar-etl get_ledger_range_from_times -s {{ ts }} --stdout -e ' + end_time,
        dag=dag,
        xcom_push=True,
    )