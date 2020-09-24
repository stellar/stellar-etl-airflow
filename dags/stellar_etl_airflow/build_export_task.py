'''
This file contains functions for creating Airflow tasks to run stellar-etl export functions.
'''

import json
from airflow import AirflowException
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from stellar_etl_airflow.default import get_default_kubernetes_affinity, get_default_kubernetes_volumes, get_default_kubernetes_volume_mounts

def get_path_variables():
    '''
        Returns the image output path, core executable path, and core config path.
    '''
    return '/etl/exported_data/', '/usr/bin/stellar-core', '/etl/stellar-core.cfg'

def select_correct_filename(cmd_type, base_name, batched_name):
    switch = {
        'archive': batched_name,
        'bucket': batched_name,
        'bounded-core': base_name,
        'unbounded-core': base_name,
    }
    filename = switch.get(cmd_type, 'No file')
    if filename == 'No file':
        raise AirflowException("Command type is not supported: ", cmd_type)
    return filename

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
    start_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"] }}'
    end_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"] }}'

    image_output_path, core_exec, core_cfg = get_path_variables()

    batch_filename = '-'.join([start_ledger, end_ledger, base_filename])
    batched_path = image_output_path + batch_filename
    base_path = image_output_path + base_filename

    correct_filename = select_correct_filename(cmd_type, base_filename, batch_filename)
    switch = {
        'archive': ['stellar-etl', command, '-s', start_ledger, '-e', end_ledger, '-o', batched_path],
        'bucket': ['stellar-etl', command, '-e', end_ledger, '-o', batched_path],
        'bounded-core': ['stellar-etl', command, '-s', start_ledger, '-e', end_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path],
        'unbounded-core': ['stellar-etl', command, '-s', start_ledger, '-x', core_exec, '-c', core_cfg, '-o', base_path],
    }

    cmd = switch.get(cmd_type, None)
    if cmd is None:
        raise AirflowException("Command type is not supported: ", cmd_type)
    return cmd, correct_filename

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

    # KubernetesPodOperators can only load JSON values into XCOM
    #output_file_dict = json.dumps({"output_file": output_file})
    output_file_dict =  '{{\\"output_file\\": \\"{filename}\\"}}'.format(filename = output_file)

    # echo the output file so it can be captured by xcom; have to run bash to combine commands
    cmd = ["/bin/sh"]# [etl_cmd[0]]#['bash', '-c']
    #args = [f'\"mkdir -p /airflow/xcom && ls /airflow\"']
    args = ["-c", f"mkdir -p {get_path_variables()[0]} && {' '.join(etl_cmd)} && mkdir -p /airflow/xcom/ && echo {output_file_dict} > /airflow/xcom/return.json && ls {get_path_variables()[0]}"]#etl_cmd[1:]#[f'\"{etl_cmd} && echo etl command finished\"']
    #args = ['\"echo \'{\\"output_file\\": \\"filename5.txt\\"}\ > /airflow/xcom/return.json\"']

    return KubernetesPodOperator(
            task_id=command + '_task',
            name=command + '_task',
            namespace='etl-tasks',
            image=Variable.get('image_name'),
            cmds=cmd,
            arguments=args, 
            dag=dag,
            do_xcom_push=True,
            is_delete_operator_pod=False,
            in_cluster=True,
            #affinity=get_default_kubernetes_affinity(),
            volume_mounts=get_default_kubernetes_volume_mounts(),
            volumes=get_default_kubernetes_volumes(),
    )

    # f'{Variable.get("output_path")}:{get_path_variables()[0]}'