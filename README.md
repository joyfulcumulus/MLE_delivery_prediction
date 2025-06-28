# Late Delivery Prediction for Ecommerce Orders (Olist)

## Introduction
This is a group project for cS611 Machine Learning Engineering Course.   

In this project, we built a Machine Learning Pipeline which 
* processes raw data from Olist into a datamart following medallion architecture (bronze, silver, gold)
* makes daily inference to predict late orders and stores the predictions into the datamart
* computes monitoring metrics and stores the metrics into the datamart
* displays the monitoring metrics into a dashboard (Jupyter notebook)

Model training is performed manually, and we create 2 models (XGBoost and Logistic Regression)

## Technologies Used
This project was built with
* Python
* Airflow
* Docker

## How to Run the Code
1. Build the Docker container

```bash
docker-compose build
```

2. Spin up the Docker container, which contains airflow-init, airflow-webserver, airflow-scheduler

```bash
docker-compose up -d
```

3. Open Airflow GUI in your internet browser and log in

4. Toggle on the DAG to start the ML pipeline

5. To remove the Docker container

```bash
docker-compose down
```

## Key Documents
* Refer to `dags/dag.py` for our task flow
* Refer to `scripts` for the Python scripts that run our airflow pipeline
* Data processing scripts are inside the `scripts/utils/` folder