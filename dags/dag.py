from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag',
    default_args=default_args,
    description='data pipeline run once a day',
    schedule_interval='0 9 * * *',  # At 00:00 on day-of-month 1
    start_date=datetime(2016, 9, 4), #2016-09-04 min date
    end_date=datetime(2017, 12, 3), #'2018-09-03' max date
    catchup=True,
) as dag:

    # data pipeline

    ############################
    # Label Store
    ############################
    dep_check_source_label_data = DummyOperator(task_id="dep_check_source_label_data")

    bronze_label_store = DummyOperator(task_id="run_bronze_label_store")

    silver_label_store = DummyOperator(task_id="silver_label_store")

    gold_label_store = BashOperator(
        task_id='run_gold_label_feature_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_label_store.py '
            '--startdate "{{ ds }}" '
        ),
    )
    label_store_completed = DummyOperator(task_id="label_store_completed")

    gold_feature_store = BashOperator(
        task_id='run_gold_feature_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_feature_store.py '
            '--startdate "{{ ds }}" '
        ),
    )
    feature_store_completed = DummyOperator(task_id="feature_store_completed")

    # Define task dependencies to run scripts sequentially
    dep_check_source_label_data >> bronze_label_store >> silver_label_store >> gold_label_store >> label_store_completed
    silver_label_store >> gold_feature_store >> feature_store_completed

    # --- model inference ---
    model_inference_start = DummyOperator(task_id="model_inference_start")

    model_1_inference = DummyOperator(task_id="model_1_inference")

    model_2_inference = DummyOperator(task_id="model_2_inference")

    model_inference_completed = DummyOperator(task_id="model_inference_completed")
    
    # Define task dependencies to run scripts sequentially
    feature_store_completed >> model_inference_start
    model_inference_start >> model_1_inference >> model_inference_completed
    model_inference_start >> model_2_inference >> model_inference_completed


    # --- model monitoring ---
    model_monitor_start = DummyOperator(task_id="model_monitor_start")

    model_1_monitor = DummyOperator(task_id="model_1_monitor")

    model_2_monitor = DummyOperator(task_id="model_2_monitor")

    model_monitor_completed = DummyOperator(task_id="model_monitor_completed")
    
    # Define task dependencies to run scripts sequentially
    model_inference_completed >> model_monitor_start
    model_monitor_start >> model_1_monitor >> model_monitor_completed
    model_monitor_start >> model_2_monitor >> model_monitor_completed
