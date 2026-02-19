import requests

url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

response = requests.get(url)
print(response.status_code)
print(response.text[:500])

