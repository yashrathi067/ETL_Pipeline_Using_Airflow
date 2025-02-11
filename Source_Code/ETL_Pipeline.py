
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from datetime import datetime
import re


default_args = {
    'owner': 'airflow',
    'retries': 1, # How many times to retry if task gets failed. (Default 0)
    'retry_delay': timedelta(minutes=5),
}


def extract_data(**kwargs):
    # File path of the data
    input_file = '/home/talentum/shared/dataset_real_estate.csv'
    
    # Read the CSV file into a DataFrame
    try:
        df = pd.read_csv(input_file)
        print(f"Data extracted from {input_file}")
        kwargs['ti'].xcom_push(key='extracted_data', value=df)
    except Exception as e:
        print(f"Error reading file {input_file}: {e}")
        kwargs['ti'].xcom_push(key='extracted_data', value=None)


def transform_data(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data', key='extracted_data')  # Pull data from XCom
    
    if df is None:
        print("No data to transform.")
        return None
    
    print('Total null values: ')
    print(df.isna().sum().sum())
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    print('Null values and duplicates dropped!!')

    # 1. Change date format by replacing '/' with '-'
    df['sale_date'] = df['sale_date'].str.replace('/', '-', regex=False)
    df['prev_sold_date'] = df['prev_sold_date'].str.replace('/', '-', regex=False)
    df['current_sale_date'] = df['sale_date']
    # Drop the old 'sale_date' column
    df = df.drop('sale_date', axis=1)
    # Convert to datetime format
    df['prev_sold_date'] = pd.to_datetime(df['prev_sold_date'], format='%d-%m-%Y', errors='coerce')
    df['current_sale_date'] = pd.to_datetime(df['current_sale_date'], format='%d-%m-%Y', errors='coerce')
    print("Date format changed successfully!!")

    # 2. Remove unwanted characters from all columns
    for column in df.columns:
        if df[column].dtype == object:  # Check if the column is a string
            df[column] = df[column].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
    print("Removal of unwanted characters successfully!!")

    # 3. Remove $ or any other symbols from price columns
    if 'price' in df.columns:
        if df['price'].dtype == 'object':  
            df['price'] = df['price'].replace({'\$': '', ',': ''}, regex=True)
    
    # Convert the price column to numeric (this handles any invalid strings by turning them into NaN)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['price'] = df['price'].round(2)
    print("Price column transformed successfully!!")

    # 4. Convert all column names to lowercase to remove case sensitivity
    df.columns = [col.lower() for col in df.columns]
    print("Lowercase columns done successfully!!")

    # 5. Adding a new column Price-Per-SqFt
    df['price_per_sqft'] = (df['price'] / df['house_size']).round(2)
    
    # 6. Adding a new column Holding Period
    # Calculate the holding period by subtracting the dates
    df['holding_period(years)'] = (((df['current_sale_date'] - df['prev_sold_date'])).dt.days/365).round(1)
    kwargs['ti'].xcom_push(key='transformed_data', value=df)
    print("Data transformed successfully.")
    return df


def load_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data', key='transformed_data')  # Pull transformed data from XCom
    
    if df is not None:
        output_file = '/home/talentum/shared/transformed_houses_data.csv'
        try:
            df.to_csv(output_file, index=False)
            print(f"Transformed data saved to {output_file}")
        except Exception as e:
            print(f"Error saving transformed data: {e}")
    else:
        print("No data to load.")


def load_data_to_s3(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data', key='transformed_data')  # Pull transformed data from XCom

    if df is not None:
        # Save the transformed data temporarily as CSV
        temp_file_path = '/home/talentum/shared/transformed_houses_data.csv'
        try:
            df.to_csv(temp_file_path, index=False)
            print(f"Transformed data saved to local file {temp_file_path}")

            # Initialize the S3 Hook to interact with AWS S3
            s3_hook = S3Hook(aws_conn_id='aws_default')  # Make sure you have your AWS connection configured in Airflow
            bucket_name = 'etl-bucket11'  # Replace with your S3 bucket name
            s3_file_key = 'transformed_houses_data.csv'  # The desired S3 key (path) for the file
            
            # Upload the local file to S3
            s3_hook.load_file(temp_file_path, s3_file_key, bucket_name=bucket_name, replace=True)
            print(f"Transformed data uploaded to S3 bucket {bucket_name} at {s3_file_key}")
            
            # Clean up the local file after upload
            #os.remove(temp_file_path)

        except Exception as e:
            print(f"Error uploading data to S3: {e}")
    else:
        print("No data to load to S3.")


with DAG(
    'local_houses_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for local house data',
    schedule_interval='@daily',  # Runs once per day
    start_date=datetime(2025, 1, 28),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )
  
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,  
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,  # Allow the task to access context (XCom)
    )

    load_task_to_s3 = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_data_to_s3,
        provide_context=True,  # Allow the task to access context (XCom)
    )


    # Task dependencies
    extract_task >> transform_task  >> load_task >> load_task_to_s3
