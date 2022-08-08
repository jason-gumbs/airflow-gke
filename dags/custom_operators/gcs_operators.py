import os
import json
import tempfile

# importing csv module
import csv
  

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class ExampleDataToGCSOperator(BaseOperator):
    """Operator that creates example JSON data and writes it to GCS.
    Args:
        task_id: (templated) sensor data left bound
        run_date: (templated) sensor data right bound
        gcp_conn_id: Airflow connection for the GCP service account
        gcs_bucket: name of the target GCS bucket
    """
    template_fields = ('run_date', )

    @apply_defaults
    def __init__(
        self,
        run_date: str,
        gcp_conn_id: str,
        gcs_bucket: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.run_date = run_date
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket

    def execute(self, context):
        """Create an example JSON and write it to a GCS bucket. """
        example_data = {'run_date': self.run_date, 'example_data': 12345}
        gcs_file_path = f"example_data_{context['ds_nodash']}.json"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, gcs_file_path)
            self.log.info(f"Writing example data to {tmp_path}.")
            with open(tmp_path, 'w') as handle:
                self.log.info(f"Writing example data to {tmp_path}.")
                json.dump(example_data, handle)

            gcs_hook = GCSHook(self.gcp_conn_id)
            gcs_hook.download(
               bucket_name=self.gcs_bucket,
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
            
            # Custom
            
            for i in range(0, len(rows)):
                rows[i][1] = rows[i][1].capitalize()
            
            with open('output.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=' ',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(fields)
                for row in rows:
                    writer.writerow(row)
            gcs_hook.upload(
               bucket_name=self.gcs_bucket,
               object_name="output.csv",
               filename="output.csv",
            )
            
            # Custom
            
            gcs_hook.upload(
               bucket_name=self.gcs_bucket,
               object_name=gcs_file_path,
               filename=tmp_path,
            )