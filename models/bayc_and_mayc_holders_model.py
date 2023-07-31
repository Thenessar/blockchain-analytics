import requests
import pandas as pd


def model(dbt, session):
    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="dbt-python"
    )

    headers = {
        'X-API-KEY': '1eLZJSGKSr2m5GnNmUojr3awSECsdqFL',
        'Content-Type': 'application/json',
    }

    json_data = {
        'options': {},
    }

    response = requests.get('https://api.transpose.io/endpoint/bayc-and-mayc-holders', headers=headers, json=json_data)
    json_data = response.json()["results"]
    final_df = pd.DataFrame(json_data)

    return final_df