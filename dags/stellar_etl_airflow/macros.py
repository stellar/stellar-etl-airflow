def subtract_data_interval(dag, time):
    last_dagrun = dag.get_last_dagrun()
    if last_dagrun is None:
        # Fallback: Use the provided time directly if no previous DAG runs exist
        return time
    interval = dag.get_run_data_interval(last_dagrun)
    return time - (interval.end - interval.start)


def batch_run_date_as_datetime_string(dag, start_time):
    return subtract_data_interval(dag, start_time).to_datetime_string()


def get_batch_id():
    return "{}-{}".format("{{ run_id }}", "{{ params.alias }}")


def batch_run_date_as_directory_string(dag, start_time):
    time = subtract_data_interval(dag, start_time)
    return (
        f"{time.year}/{time.month}/{time.day}/{time.hour}:{time.minute}:{time.second}"
    )
