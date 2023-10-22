import os
import sys
import pandas as pd
from google.cloud import bigquery

def main():
    client = bigquery.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    query = f"""
        SELECT
            {os.environ['COLUMN_LIST']}
        FROM 
            '{os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}'
        WHERE
            publish_date > '{os.environ['START_DATE']}' 
        AND 
            publish_date < '{os.environ['END_DATE']}
    """
    query_job = client.query(query)
    result = query_job.result()
    output = result.to_dataframe()
    output.to_csv('output.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    main()
