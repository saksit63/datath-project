from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import requests


MYSQL_CONNECTION = "mysql_default"
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"

mysql_output_path = "/home/airflow/gcs/data/transaction_data_merged.parquet"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.parquet"
final_output_path = "/home/airflow/gcs/data/final_output.parquet"

@task()
def get_data_from_mysql(output_path):
    
    #เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    product = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.product")
    customer = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.customer")
    transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.transaction")

    #merge data
    merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")

    #save file to parquet
    merged_transaction.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")

@task()
def get_conversion_rate(output_path):

    # ใช้ request ไป get ข้อมูลจาก CONVERSION_RATE_URL
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.drop(columns=['id'])

    #แปลงคอลัม date ให้เป็น datetime
    df['date'] = pd.to_datetime(df['date'])

    #save file to parquet
    df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")

@task()
def merge_data(transaction_path, conversion_rate_path, output_path):
    
    #read file
    transaction = pd.read_parquet(transaction_path)
    conversion_rate = pd.read_parquet(conversion_rate_path)

    #merge data
    final_df = transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")

    # แปลงราคา ให้เป็น total_amount และ thb_amount
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]

    # drop column ที่ไม่ใช้
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)

    #เปลี่ยนชื่อคอลัม
    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
        'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']

    #save file to parquet
    final_df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")


default_args = {
    'owner': 'Saksit',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'project_pipeline_datath',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:
    

    t1 = get_data_from_mysql(mysql_output_path)

    t2 = get_conversion_rate(conversion_rate_output_path)

    t3 = merge_data(
        transaction_path=mysql_output_path,
        conversion_rate_path=conversion_rate_output_path,
        output_path=final_output_path
    )

    t4 = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket="us-central1-composer-airflo-2fb7918a-bucket",
        source_objects=["data/final_output.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="final_workshop.transaction",
        write_disposition="WRITE_TRUNCATE"
    )
    
    
    [t1, t2] >> t3 >> t4
