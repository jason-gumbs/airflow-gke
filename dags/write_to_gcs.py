"""DAG with a custom operator that creates and writes example data to GCS. """

from airflow import DAG
from datetime import datetime
import csv
# from custom_operators.gcs_operators import ExampleDataToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

with DAG(
    'create_and_write_example_data_to_gcs',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
) as dag:
    
    # create_and_write_example_data = ExampleDataToGCSOperator(
    #     task_id='create_example_data',
    #     run_date='{{ ds }}',
    #     gcp_conn_id='airflow_gke_gcs_conn_id',
    #     gcs_bucket='my-bucket-codit'
    # )
    gcp_conn_id='airflow_gke_gcs_conn_id'
    gcs_bucket='my-bucket-codit'
    gcs_hook = GCSHook(gcp_conn_id)
    gcs_hook.download(
               bucket_name=gcs_bucket,
               object_name="test.csv",
               filename="test.csv",
            )

    fields = []
    rows = []
    
    # reading csv file
    with open('test.csv', 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
    
        # extracting each data row one by one
        for row in csvreader:
            rows.append(row)
    
        # get total number of rows
        print("Total no. of rows: %d"%(csvreader.line_num))
    
    # printing the field names
    print('Field names are:' + ', '.join(field for field in fields))
    
    # printing first 5 rows
    print('\nFirst 5 rows are:\n')
    for row in rows[:5]:
        # parsing each column of a row
        for col in row:
            print("%10s"%col,end=" "),
        print('\n')
    # gcs_hook.upload(
    #     bucket_name=self.gcs_bucket,
    #     object_name=gcs_file_path,
    #     filename=tmp_path,
    # )