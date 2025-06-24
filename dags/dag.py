from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True, # if job failed day before, cannot continue (ensures safety)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag',
    default_args=default_args,
    description='Delivery lateness prediction pipeline',
    schedule_interval='0 9 * * *',  # At 09:00 AM on daily
    start_date=datetime(2016, 9, 4), #min date
    end_date=datetime(2016, 10, 4), # end_date=datetime(2017, 12, 3), #'2018-09-03' max date
    catchup=True,
    max_active_runs=1 # ensures no parallel processing. Will execute all steps for day 1 first, then move to day 2
) as dag:

    ###########################
    #Data Processing
    ###########################
    bronze_store_start = DummyOperator(task_id="bronze_store_start")

    bronze_store_run = BashOperator(
        task_id='bronze_store_run',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 bronze_store.py "{{ ds }}"'
        ),
    )
    bronze_store_completed = DummyOperator(task_id="bronze_store_completed")

    silver_store_run = BashOperator(
        task_id='silver_store_run',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 silver_store.py '
            '--startdate "{{ ds }}" '
        ),
        execution_timeout=timedelta(minutes=30),  
        retries=1,
        retry_delay=timedelta(minutes=5),
        dag=dag,
    )  
    silver_store_completed = DummyOperator(task_id="silver_store_completed")  

    ###########################
    #Label Store
    ###########################
    gold_label_store = BashOperator(
        task_id='run_gold_label_feature_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_label_store.py '
            '--startdate "{{ ds }}" '
        ),
    )
    label_store_completed = DummyOperator(task_id="label_store_completed")

    ###########################
    #Feature Store
    ###########################
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
    bronze_store_start >> bronze_store_run >> bronze_store_completed
    bronze_store_completed >> silver_store_run >> silver_store_completed
    silver_store_completed >> gold_label_store >> label_store_completed
    silver_store_completed >> gold_feature_store >> feature_store_completed

    ###########################
    #Model Inference
    ###########################
    model_inference_start = DummyOperator(task_id="model_inference_start")

    model_reg_inference = BashOperator(task_id='model_reg_inference',
        bash_command=(
            'cd /opt/airflow/scripts &&'
            'python3 model_inference.py '
            '--snapshotdate "{{ ds }}" '
            '--modelname reg_2017_12_04.pkl'
        ),
    )

    model_xgb_inference = BashOperator(task_id='model_xgb_inference',
        bash_command=(
            'cd /opt/airflow/scripts &&'
            'python3 model_inference.py '
            '--snapshotdate "{{ ds }}" '
            '--modelname xgb_2017_12_04.pkl'
        ),
    )

    model_inference_completed = DummyOperator(task_id="model_inference_completed")
    
    # Define task dependencies to run scripts sequentially
    feature_store_completed >> model_inference_start
    model_inference_start >> model_reg_inference >> model_inference_completed
    model_inference_start >> model_xgb_inference >> model_inference_completed


    ###########################
    #Model Monitoring
    ###########################
    def is_last_run(execution_date_str):
        return execution_date_str >= "2017-06-01" #start monitoring 6 months before 

    model_monitor_start = ShortCircuitOperator(
        task_id="model_monitor_start",
        python_callable=is_last_run,
        op_args=["{{ ds }}"]
    )


    model_xgb_monitor = BashOperator(task_id='model_xgb_monitor',
        bash_command=(
            'cd /opt/airflow/scripts &&'
            'python3 model_monitoring.py '
            '--snapshotdate "{{ ds }}" '
            '--model xgb'
        ),
    )

    model_reg_monitor = BashOperator(task_id='model_reg_monitor',
        bash_command=(
            'cd /opt/airflow/scripts &&'
            'python3 model_monitoring.py '
            '--snapshotdate "{{ ds }}" '
            '--model reg'
        ),
    )
    model_monitor_completed = DummyOperator(task_id="model_monitor_completed")
    
    # Define task dependencies to run scripts sequentially
    model_inference_completed >> model_monitor_start
    model_monitor_start >> model_xgb_monitor >> model_monitor_completed
    model_monitor_start >> model_reg_monitor >> model_monitor_completed
