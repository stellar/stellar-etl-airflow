'''
This file contains functions for creating Airflow tasks to run stellar-etl export functions.
In order for these tasks to work, the stellar-etl must be installed, and the GOBIN needs
to be added to the PATH env variable.
'''

import json
import logging

from subprocess import Popen, PIPE

from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

def parse_ledger_range(context):
    '''
    Reads in the output of the get_ledger_range_from_times task, which is a JSON object 
    containing a start and end field. Converts the fields to strings and returns them.
    
    Parameters:
        context - the context object passed by Airflow (requires provide_context=True when creating the operator)
    Returns:
        start and end ledger sequence numbers
    '''

    range_string = context['task_instance'].xcom_pull(task_ids='get_ledger_range_from_times')
    range_parsed = json.loads(range_string)
    start = range_parsed['start']
    end = max(range_parsed['end'] - 1, start)
    return str(start), str(end)

def execute_cmd(args):    
    '''
    Executes the provided arguments on the command line. Raises an AirflowException if the return code is non-zero, 
    which indicates a failure.
    
    Parameters:
        context - the context object passed by Airflow (requires provide_context=True when creating the operator)
    Returns:
        output of the command, error
    '''
    
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if process.returncode:
        raise AirflowException("Bash command failed", process.returncode, stderr)

def get_path_variables():
    return Variable.get('output_path'), Variable.get('core_exec_path'), Variable.get('core_cfg_path')

def run_etl_cmd(command, base_filename, cmd_type, **kwargs):
    '''
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
        name of the file that contains the exported data
    '''

    start_ledger, end_ledger = parse_ledger_range(kwargs)
    output_path, core_exec, core_cfg = get_path_variables()

    batch_filename = '-'.join([start_ledger, end_ledger, base_filename])
    batched_path = output_path + batch_filename
    base_path = output_path + base_filename
    cmd_args = ['stellar-etl', command]

    if cmd_type == 'archive':
        cmd_args.extend(['-s', start_ledger, '-e', end_ledger, '-o', batched_path])
    elif cmd_type == 'bucket':
        cmd_args.extend(['-e', end_ledger, '-o', batched_path])
    elif cmd_type == 'bounded-core':
        cmd_args.extend(['-s', start_ledger, '-e', end_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path])
    elif cmd_type == 'unbounded-core':
        cmd_args.extend(['-s', start_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path])
    else:
        raise AirflowException("Command type is not supported: ", cmd_type)
    execute_cmd(cmd_args)
    return batch_filename

def build_export_task(dag, cmd_type, command, filename):
    '''
    Creates a task that calls the provided stellar-etl export function with the correct arguments.
    
    Parameters:
        dag - the parent dag
        cmd_type - the type of the command, which is determined by the information source
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        filename - filename for the output file or folder
    Returns:
        the newly created task
    '''

    return PythonOperator(
            task_id=command + '_task',
            python_callable=run_etl_cmd,
            op_kwargs={'command': command, 'base_filename': filename, 'cmd_type': cmd_type},
            provide_context=True,
            dag=dag,
        )