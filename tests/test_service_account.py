from google.cloud import bigquery
from google.oauth2 import service_account
import os

KEY_PATH = "../keys/jcdeah-006-e1b616f9939c.json"

#Tester for service acount bigquery connectione
#To see if service account have bigquery permission

credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
    project="jcdeah-006",  
    credentials=credentials,
)

query = """
SELECT name, SUM(number) as total_people
FROM `bigquery-public-data.usa_names.usa_1910_current`
WHERE year = 2020
GROUP BY name
ORDER BY total_people DESC
LIMIT 10
"""

query_job = client.query(query)  

print("Top 10 baby names in the US in 2020:")
for row in query_job:
    print(f"{row.name}: {row.total_people}")