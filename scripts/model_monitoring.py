import argparse
import os
import glob
import pandas as pd
import pyspark
import pyspark.sql.functions as F
from sklearn.metrics import fbeta_score
from pyspark.sql.functions import col
from tqdm import tqdm

def monitoring(snapshotdate, model):
    snapshotdate_str = snapshotdate.replace('-', '_')
    if model == 'reg':
        model_pred_loc = "datamart/gold/model_predictions/reg_2018_04_01/" #update
        file_name = 'reg_2018_04_01_predictions_' + snapshotdate_str + '.parquet' #update
    else:
        model_pred_loc = "datamart/gold/model_predictions/xgb_2018_04_01/" #update
        file_name = 'xgb_2018_04_01_predictions_' + snapshotdate_str + '.parquet' #update
    
    date_lst = [snapshotdate]
    for date_str in tqdm(date_lst, total=len(date_lst), desc="Saving labels"):
        files_list = os.path.join(model_pred_loc, file_name)
        if os.path.exists(files_list):
            model_pred_list = spark.read.parquet(files_list)
            print("row_count for predictions:",model_pred_list.count(),"\n")
            #Import the ground truths
            ground_truth_loc =  "datamart/gold/label_store"
            files_list = glob.glob(os.path.join(ground_truth_loc, snapshotdate_str+'*.parquet'))
            ground_truth_list = spark.read.option("header", "true").parquet(*files_list)
            print("row_count for ground Truths:",ground_truth_list.count(),"\n")
            rows = model_pred_list.count()
            ground_truth = ground_truth_list.toPandas()
            model_pred = model_pred_list.toPandas()
            if ground_truth_list.count() == 0:
                df_results = pd.DataFrame(columns=['snapshot_date', 'model', 'num_of_orders','f1_5_score'])
            else:
                final_df = (
                    ground_truth.merge(model_pred, how='left', on='order_id')
                    [['order_id','model_predictions','miss_delivery_sla','model_name','snapshot_date_y']]  # select columns
                    .rename(columns={'miss_delivery_sla': 'ground_truth'})  # rename
                )
                # Drop NaNs and ensure integer types
                filtered_eval = final_df.dropna(subset=["ground_truth", "model_predictions"])
                y_true = filtered_eval["ground_truth"].astype(int)
                y_pred = filtered_eval["model_predictions"].astype(int)

                # Compute F1.5 score
                f1_5_score = fbeta_score(y_true, y_pred, beta=1.5)
                print(f"F1.5-score: {f1_5_score:.4f}")
                df_results = pd.DataFrame([[snapshotdate, model,rows, f1_5_score]], columns=['snapshot_date', 'model', 'num_of_orders', 'f1_5_score'])

        else:
            df_results = pd.DataFrame(columns=['snapshot_date', 'model', 'num_of_orders','f1_5_score'])

    # --- save model inference to datamart gold table ---
    if model == 'reg':
        gold_directory = f"datamart/gold/model_monitoring/reg/"
    else:
        gold_directory = f"datamart/gold/model_monitoring/xgb/"
    
    if not os.path.exists(gold_directory):
        os.makedirs(gold_directory)

    # save gold table
    partition_name = snapshotdate_str+'.parquet'
    filepath = gold_directory + partition_name
    if df_results.empty:
        print("No data to write. Skipping write step.")
        spark.stop()
    else: 
        spark_df = spark.createDataFrame(df_results)
        spark_df = spark_df.withColumn("f1_5_score", col("f1_5_score").cast("double"))
        spark_df.write.mode("overwrite").parquet(filepath)
    rows = df_results.shape[0]
    print('saved to:', filepath)
    return rows

    #return snapshotdate
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
    parser.add_argument("--model", type=str, required=True, help="model")
    args = parser.parse_args()

    ############################
    # Model Monitoring Start
    ############################
    print('\n\n---starting job---\n\n')

    # Call main with arguments explicitly passed
    results = monitoring(args.snapshotdate, args.model)

    print(f'rows added {results:.4f} for {args.snapshotdate}')
    
    print('\n\n---completed job---\n\n')

    ############################
    # Model Monitoring End
    ############################
