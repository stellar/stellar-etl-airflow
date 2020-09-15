'''
This file contains functions for creating Airflow tasks to run stellar-etl export functions.
'''

import json
import logging

from airflow import AirflowException
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator


def get_path_variables():
    return Variable.get('image_output_path'), Variable.get('core_exec_path'), Variable.get('core_cfg_path')

def generate_etl_cmd(command, base_filename, cmd_type):
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
        the generated etl command; name of the file that contains the exported data
    '''

    # These are JINJA templates, which are filled by airflow at runtime. The string from get_ledger_range_from_times is pulled from XCOM. 
    # Then, it is turned into a JSON object with fromjson, and then the start or end field is accessed.
    start_ledger = '{{(ti.xcom_pull(task_ids="get_ledger_range_from_times") | fromjson)["start"]}}'
    end_ledger = '{{(ti.xcom_pull(task_ids="get_ledger_range_from_times") | fromjson)["end"]}}'

    image_output_path, core_exec, core_cfg = get_path_variables()

    batch_filename = '-'.join([start_ledger, end_ledger, base_filename])
    batched_path = image_output_path + batch_filename
    base_path = image_output_path + base_filename
    full_cmd = f'stellar-etl {command} '

    switch = {
        'archive': (f'stellar-etl {command} -s {start_ledger} -e {end_ledger} -o {batched_path}', batch_filename),
        'bucket': (f'stellar-etl {command} -e {end_ledger} -o {batched_path}', batch_filename),
        'bounded-core': (f'stellar-etl {command} -s {start_ledger} -e {end_ledger} -x {core_exec} -c {core_cfg} -o {base_path}', base_filename),
        'unbounded-core': (f'stellar-etl {command} -s {start_ledger} -x {core_exec} -c {core_cfg} -o {base_path}', base_filename),
    }

    cmd, filename = switch.get(cmd_type, ('No command', 'No file'))
    if cmd == 'No command':
        raise AirflowException("Command type is not supported: ", cmd_type)
    return cmd, filename

def build_export_task(dag, cmd_type, command, filename):
    '''
    Creates a task that calls the provided export function with the correct arguments in the stellar-etl Docker image.
    
    Parameters:
        dag - the parent dag
        cmd_type - the type of the command, which is determined by the information source
        command - stellar-etl command (ex. export_ledgers, export_accounts)
        filename - filename for the output file or folder
    Returns:
        the newly created task
    '''

    etl_cmd, output_file = generate_etl_cmd(command, filename, cmd_type)

    # echo the output file so it can be captured by xcom; have to run bash to combine commands
    full_cmd = f'bash -c "{etl_cmd} && echo {output_file}"'

    return DockerOperator(
            task_id=command + '_task',
            image=Variable.get('image_name'),
            command=full_cmd, 
            volumes=[f'{Variable.get("output_path")}:{Variable.get("image_output_path")}'],
            dag=dag,
            do_xcom_push=True,
            auto_remove=True,
        )