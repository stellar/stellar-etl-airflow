import json

from datetime import timedelta
from subprocess import Popen, PIPE

from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def parse_ledger_range(context):
    range_string = context['task_instance'].xcom_pull(task_ids='get_ledger_range_from_times')
    range_parsed = json.loads(range_string)
    start = range_parsed['start']
    end = range_parsed['end']
    return str(start), str(end)

def execute_cmd(args):
    process = Popen(args)#, stdout=PIPE, stderr=PIPE)
    if process.returncode:
        raise AirflowException("Bash command failed")
    stdout, stderr = process.communicate()
    return stdout, stderr

def get_path_variables():
    return Variable.get('output_path'), Variable.get('core_exec_path'), Variable.get('core_cfg_path')

def run_etl_cmd(command, filename, cmd_type, **kwargs):
    start_ledger, end_ledger = parse_ledger_range(kwargs)
    output_path, core_exec, core_cfg = get_path_variables()
    cmd_args = ['stellar-etl', command, '-o', output_path + filename]

    if cmd_type == 'archive':
        cmd_args.extend(['-s', start_ledger, '-e', end_ledger])
    elif cmd_type == 'bucket':
        cmd_args.extend(['-e', end_ledger])
    elif cmd_type == 'bounded-core':
        cmd_args.extend(['-s', start_ledger, '-e', end_ledger, '-x', core_exec, '-c', core_cfg,])
    elif cmd_type == 'unbounded-core':
        cmd_args.extend(['-s', start_ledger, '-x', core_exec, '-c', core_cfg, ])
    else:
        raise AirflowException("Command type is not supported: ", cmd_type)

    return execute_cmd(cmd_args)

def build_export_task(dag, cmd_type, command, filename):
    return PythonOperator(
            task_id=command + '_task',
            python_callable=run_etl_cmd,
            op_kwargs={'command': command, 'filename': filename, 'cmd_type': cmd_type},
            provide_context=True,
            dag=dag,
        )