from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

SUGGESTED_KEYWORDS_VAR_NAME = "SUGGESTED_KEYWORDS"
CHILD_DAG_START_DATE_VAR_NAME = "CHILD_DAG_START_DATE"


def get_suggested_keywords():
    return Variable.get(SUGGESTED_KEYWORDS_VAR_NAME).split(",")


def get_parameters(**kwargs):
    suggested_keywords = get_suggested_keywords()

    parameters = {
        "suggested_keywords": Param(
            suggested_keywords,
            type="array",
            examples=suggested_keywords)
    }
    return parameters


def save_suggested_keywords_to_var(**kwargs):
    parameters = kwargs["params"]
    Variable.set(SUGGESTED_KEYWORDS_VAR_NAME, ",".join(parameters["suggested_keywords"]))
    Variable.set(CHILD_DAG_START_DATE_VAR_NAME, kwargs["execution_date"].isoformat())


# Define the default arguments and the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    'select_keywords',
    default_args=default_args,
    description='Keywords selection DAG',
    schedule_interval=None,
    params=get_parameters()
)

save_selected_keywords_task = PythonOperator(
    task_id='save_selected_keywords',
    python_callable=save_suggested_keywords_to_var,
    provide_context=True,
    dag=dag,
)

save_selected_keywords_task
