"""
This DAG triggers state_table_export and history_table_export DAGs in hourly chunks
to backfill large date ranges without hitting GCS upload limits.

Usage:
- Trigger with params: backfill_start_date, backfill_end_date, chunk_hours
- Example: {"backfill_start_date": "2025-12-17T17:25:00+00:00", 
            "backfill_end_date": "2026-01-08T00:00:00+00:00",
            "chunk_hours": 3}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()


def generate_date_chunks(**context):
    """Generate date chunks based on params"""
    params = context['params']
    start_str = params['backfill_start_date']
    end_str = params['backfill_end_date']
    chunk_hours = params['chunk_hours']
    alias = params.get('alias', 'cc')
    
    # Parse timestamps
    start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
    
    chunks = []
    current = start
    
    while current < end:
        chunk_end = min(current + timedelta(hours=chunk_hours), end)
        chunks.append({
            'start': current.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
            'end': chunk_end.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
            'alias': alias,
        })
        current = chunk_end
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='chunks', value=chunks)
    return len(chunks)


dag = DAG(
    'backfill_controller',
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 12, 17),
    description='Controller DAG to backfill state and history tables in chunks',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['backfill', 'controller'],
    params={
        'backfill_start_date': Param(
            default='2025-12-17T17:25:00+00:00',
            type='string',
            description='Start timestamp in RFC3339 format (e.g., 2025-12-17T17:25:00+00:00)',
        ),
        'backfill_end_date': Param(
            default='2026-01-08T00:00:00+00:00',
            type='string',
            description='End timestamp in RFC3339 format (e.g., 2026-01-08T00:00:00+00:00)',
        ),
        'chunk_hours': Param(
            default=3,
            type='integer',
            description='Number of hours per chunk (1 = hourly, 3 = 3 hours, 6 = 6 hours, 24 = daily)',
        ),
        'alias': Param(
            default='cc',
            type='string',
            description='Alias for the export (default: cc)',
        ),
        'trigger_state_table': Param(
            default='true',
            type='string',
            description='Whether to trigger state_table_export (true/false)',
        ),
        'trigger_history_table': Param(
            default='true',
            type='string',
            description='Whether to trigger history_table_export (true/false)',
        ),
    },
)

# Generate the list of date chunks
generate_chunks = PythonOperator(
    task_id='generate_date_chunks',
    python_callable=generate_date_chunks,
    dag=dag,
)


def create_trigger_tasks(dag_id, task_id_prefix):
    """Create trigger tasks for a specific DAG"""
    def trigger_chunk(chunk_index, **context):
        """Trigger a single chunk"""
        chunks = context['task_instance'].xcom_pull(
            task_ids='generate_date_chunks', 
            key='chunks'
        )
        chunk = chunks[chunk_index]
        
        return {
            'alias': chunk['alias'],
            'manual_start_date': chunk['start'],
            'manual_end_date': chunk['end'],
        }
    
    tasks = []
    # Create up to 30 tasks (for up to 30 days with daily chunks)
    # Adjust this number if you need more
    for i in range(30):
        task = TriggerDagRunOperator(
            task_id=f'{task_id_prefix}_chunk_{i:02d}',
            trigger_dag_id=dag_id,
            conf="{{ task_instance.xcom_pull(task_ids='generate_date_chunks', key='chunks')[" + str(i) + "] }}",
            wait_for_completion=True,
            poke_interval=30,
            execution_date="{{ execution_date }}",
            reset_dag_run=True,
            dag=dag,
        )
        
        if i == 0:
            generate_chunks >> task
        else:
            tasks[-1] >> task
        
        tasks.append(task)
    
    return tasks


# Note: Due to Airflow's dynamic task generation limitations, 
# this simplified version triggers up to 30 chunks sequentially.
# For production, consider using Airflow's dynamic task mapping (Airflow 2.3+)

# For now, let's create a simpler approach using a loop in Python operator

def trigger_backfill(**context):
    """Trigger all chunks sequentially"""
    from airflow.api.common.trigger_dag import trigger_dag
    import time
    
    params = context['params']
    start_str = params['backfill_start_date']
    end_str = params['backfill_end_date']
    chunk_hours = params['chunk_hours']
    alias = params.get('alias', 'cc')
    trigger_state = params.get('trigger_state_table', 'true') == 'true'
    trigger_history = params.get('trigger_history_table', 'true') == 'true'
    
    # Parse timestamps
    start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
    
    current = start
    chunk_num = 0
    
    while current < end:
        chunk_end = min(current + timedelta(hours=chunk_hours), end)
        
        conf = {
            'alias': alias,
            'manual_start_date': current.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
            'manual_end_date': chunk_end.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        }
        
        print(f"\nChunk {chunk_num}: {conf['manual_start_date']} to {conf['manual_end_date']}")
        
        # Create unique run_id with timestamp to avoid conflicts
        run_id_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Trigger state_table_export
        if trigger_state:
            print(f"  Triggering state_table_export...")
            trigger_dag(
                dag_id='state_table_export',
                run_id=f"backfill_{run_id_timestamp}_chunk{chunk_num:02d}_state",
                conf=conf,
                replace_microseconds=False,
            )
        
        # Trigger history_table_export
        if trigger_history:
            print(f"  Triggering history_table_export...")
            trigger_dag(
                dag_id='history_table_export',
                run_id=f"backfill_{run_id_timestamp}_chunk{chunk_num:02d}_history",
                conf=conf,
                replace_microseconds=False,
            )
        
        # Small sleep to ensure unique timestamps for next iteration
        time.sleep(1)
        
        current = chunk_end
        chunk_num += 1
    
    print(f"\nTotal chunks triggered: {chunk_num}")
    return chunk_num


trigger_all = PythonOperator(
    task_id='trigger_all_chunks',
    python_callable=trigger_backfill,
    dag=dag,
)

generate_chunks >> trigger_all