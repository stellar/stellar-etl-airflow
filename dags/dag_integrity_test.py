from os import path

from airflow.models import DagBag

root = path.dirname(path.dirname(__file__))


def test_dagbag():
    dag_bag = DagBag(include_examples=False, dag_folder=f"{root}/dags/")
    assert not dag_bag.import_errors
