from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# -----------------------------
# Default arguments
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id='orders_backfilling_dag',
    description='Run Spark backfilling job on existing Dataproc cluster',
    start_date=datetime(2024, 12, 14),
    schedule=None,
    catchup=False,
    tags=['dev'],
    default_args=default_args,
    params={
        'execution_date': Param(
            default='NA',
            type='string',
            description='Execution date in yyyymmdd format'
        ),
    },
) as dag:
    
    
    # -----------------------------
    # Read cluster details from Airflow Variable
    # -----------------------------
    config = Variable.get("cluster_details", deserialize_json=True)

    CLUSTER_NAME = config['CLUSTER_NAME']
    PROJECT_ID = config['PROJECT_ID']
    REGION = config['REGION']

    # -----------------------------
    # Python function to resolve execution date
    # -----------------------------
    def get_execution_date(ds_nodash, **context):
        execution_date = context['params'].get('execution_date', 'NA')
        return ds_nodash if execution_date == 'NA' else execution_date

    get_execution_date_task = PythonOperator(
        task_id='get_execution_date',
        python_callable=get_execution_date,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    # -----------------------------
    # Submit PySpark job to existing cluster
    # -----------------------------
    pyspark_job_file = (
        'gs://airflow-projects-de/airflow-project-02/'
        'spark_code/orders_data_process.py'
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": CLUSTER_NAME
            },
            "pyspark_job": {
                "main_python_file_uri": pyspark_job_file,
                "args": [
                    "--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"
                ],
            },
        },
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    get_execution_date_task >> submit_pyspark_job
