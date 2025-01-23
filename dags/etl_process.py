from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from etl.elt_processes import load_data, import_payment_data,add_payment_amount_data


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
        python_callable=load_data
    )

    start_dag
    

with DAG(

    dag_id=f'import_payment_data',
    schedule='0 22 * * *',
    default_args=default_args,
    tags=['etl']
) as dag:
    

    start_dag=PythonOperator(
        task_id="start_dag",
        python_callable=add_payment_amount_data
    )

    start_dag
