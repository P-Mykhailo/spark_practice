import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mykh_key.json'

client = bigquery.Client()

sql_query = """
SELECT * 
FROM `extreme-arch-376708.spark_pr.players` 
LIMIT 1000
"""

query_job = client.query(sql_query)

for row in query_job.result():
    print(row)