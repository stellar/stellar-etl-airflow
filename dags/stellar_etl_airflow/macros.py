def subtract_data_interval(dag, time):
    interval = dag.get_run_data_interval(dag.get_last_dagrun())
    return time - (interval.end - interval.start)

def batch_run_date_as_datetime_string(dag, start_time):
    return subtract_data_interval(dag, start_time).to_datetime_string()

def get_batch_id():
    return '{}-{}'.format('{{ run_id }}', '{{ params.alias }}')