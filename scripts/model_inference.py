import argparse
import os
import glob
import pandas as pd
import pickle
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
from functools import reduce
from sklearn.preprocessing import OneHotEncoder


import pyspark.sql.functions as F

from pyspark.sql.functions import col


# to call this script: python model_train.py --snapshotdate "2024-09-01"

def main(snapshotdate, modelname):
    # --- set up config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["snapshot_date"] = datetime.strptime(config["snapshot_date_str"], "%Y-%m-%d")
    config["model_name"] = modelname
    config["model_bank_directory"] = "model_bank/"
    config["model_artefact_filepath"] = config["model_bank_directory"] + config["model_name"]
    
    pprint.pprint(config)
    
    # --- load model artefact from model bank ---
    # Load the model from the pickle file
    with open(config["model_artefact_filepath"], 'rb') as file:
        model_artefact = pickle.load(file)
    
    print("Model loaded successfully! " + config["model_artefact_filepath"])

    # --- load feature store ---
    feature_location = "datamart/gold/feature_store/"
    
    # Load parquet into DataFrame - connect to feature store
    files_list = glob.glob(os.path.join(feature_location, '*.parquet'))
    features_store_sdf = spark.read.option("header", "true").parquet(*files_list)
    print("row_count for features:",features_store_sdf.count(),"\n")
    
    # Filter out NA
    features_store_sdf = features_store_sdf.drop("snapshot_date","avg_delay_rate","concentration","act_days_to_deliver","total_freight_value","avg_processing_time","same_state","total_volume_cm3","seller_city","seller_state")
    rows_with_nulls = features_store_sdf.filter(
        reduce(lambda a, b: a | b, (col(c).isNull() for c in features_store_sdf.columns))
    )
    order_ids_to_drop = [row["order_id"] for row in rows_with_nulls.select("order_id").distinct().collect()]
    features_store_sdf = features_store_sdf.filter(~col("order_id").isin(order_ids_to_drop))
    features_store_sdf = features_store_sdf.filter(col("order_status") == "delivered")
    features_sdf = features_store_sdf.toPandas()
    print("extracted features_sdf", features_sdf.count(), config["snapshot_date"])
    
    if features_sdf.empty:
        y_inference_pdf = pd.DataFrame(columns=['order_id', 'order_status', 'model_name', 'model_predictions'])
        print('y_inference', y_inference_pdf.shape[0])
    else: 
        # prepare X_inference
        encoder = OneHotEncoder(drop = 'first', sparse=False, handle_unknown='ignore')
        #encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        encoder.fit(features_sdf[['season']])  # Only fit on training data
        encoded_feature = encoder.transform(features_sdf[['season']])
        encoded_f = pd.DataFrame(encoded_feature, columns=encoder.get_feature_names_out(['season']), index=features_sdf.index)
        features_sdf = pd.concat([features_sdf.drop(columns=['season']), encoded_f], axis=1)
        features_pdf = features_sdf.select_dtypes(include='number')
        
        #features_pdf = features_sdf.drop(columns=['order_id', 'order_status']).values
        # apply transformer - standard scaler
        transformer_stdscaler = model_artefact["preprocessing_transformers"]["stdscaler"]
        X_inference = transformer_stdscaler.transform(features_pdf)
        print('X_inference', X_inference.shape[0])
 
        # --- model prediction inference ---
        # load model
        model = model_artefact["model"]
        threshold = model_artefact['threshold']
        
        # predict model
        y_inference = model.predict_proba(X_inference)[:, 1]
        
        # prepare output
        y_inference_pdf = features_sdf[["order_id","order_status",]].copy()
        y_inference_pdf["model_predictions"] = y_inference.round(4)
        y_inference_pdf["model_predictions"] = (y_inference_pdf["model_predictions"] > threshold).astype(int)
        y_inference_pdf["snapshot_date"] = snapshotdate
        y_inference_pdf["model_name"] = config["model_name"]
        row_count = y_inference_pdf.shape[0]

    # --- save model inference to datamart gold table ---
    # create bronze datalake
    gold_directory = f"datamart/gold/model_predictions/{config['model_name'][:-4]}/"
    print(gold_directory)
    
    if not os.path.exists(gold_directory):
        os.makedirs(gold_directory)
    
    # save gold table - IRL connect to database to write
    partition_name = config["model_name"][:-4] + "_predictions_" + config["snapshot_date_str"].replace('-','_') + '.parquet'
    filepath = gold_directory + partition_name
    if y_inference_pdf.empty:
        print("No inference data to write. Skipping write step.")
        spark.stop()
        return

    spark.createDataFrame(y_inference_pdf).write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    print(f'rows added: {row_count}')

    # --- end spark session --- 
    spark.stop()


if __name__ == "__main__":
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--modelname", type=str, required=True, help="model_name")
    args = parser.parse_args()

    ############################
    # Model Inference Start
    ############################
    print('\n\n---starting job---\n\n')

    # Call main with arguments explicitly passed
    main(args.snapshotdate, args.modelname)
    
    print('\n\n---completed job---\n\n')

    ############################
    # Model Inference End
    ############################
