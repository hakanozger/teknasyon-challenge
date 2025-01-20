from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from etl.elt_processes import call


default_args ={
    'owner':'hakanozger',
    'start_date': datetime(2025,1,1)
}


with DAG(

    dag_id=f'etl_process',
    schedule='0 22 * * *',
    default_args=default_args,
    tags=['etl']
) as dag:
    

    start_dag=PythonOperator(
        task_id="start_dag",
        python_callable=call
    )

    start_dag
    
