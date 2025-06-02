import requests
import time
import os
import pandas as pd
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from supabase import create_client, Client

class SubgraphClient:
    """A class to handle subgraph API requests."""

    def __init__(self, url, headers):
        self.url = url
        self.headers = headers

    def fetch_subgraph_data(self, query, method):
        all_results = []
        skip = 0
        batch_size = 1000

        while True:
            print(f"Fetching transactions with skip={skip}...")

            response = requests.post(
                url = self.url,
                headers = self.headers,
                json={"query": query, "variables": {"skip": skip}}
            )

            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            transactions = data.get("data", {}).get(method, [])

            if not transactions:
                print("No more records found.")
                break

            all_results.extend(transactions)
            skip += batch_size

            time.sleep(0.5)

        return all_results
    
    SUBGRAPH_HEADERS = {
        "Content-Type": "application/json",
    }
    
    MEZO_PORTAL_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-portal-mainnet/1.0.0/gn'
    MUSD_MARKET_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/market-mezo/1.0.0/gn'
    BORROWER_OPS_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/borrower-operations-mezo/1.0.0/gn"
    MUSD_TOKEN_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-token/1.0.0/gn"
    MUSD_STABILITY_POOL_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-stability-pool/1.0.0/gn"
    MUSD_TROVE_MANAGER_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-trove-manager/1.0.0/gn"

class SupabaseClient:

    def __init__(self):
        # Fetch credentials for reading data from production database
        self.url: str = os.getenv("SUPABASE_URL_PROD") 
        self.key: str = os.getenv("SUPABASE_KEY_PROD")
        self.supabase: Client = create_client(self.url, self.key)

        # Fetch credentials for inserting data to data science database
        self.insert_url: str = os.getenv("SUPABASE_DATA_URL")
        self.insert_key: str = os.getenv("SUPABASE_DATA_KEY")
        self.supabase_insert: Client = create_client(self.insert_url, self.insert_key)
    
    def fetch_table_data(self, table_name) -> pd.DataFrame:
        response = self.supabase.table(table_name).select("*").execute()
        data = response.data
        df = pd.DataFrame(data)
        return df
    
    def fetch_rpc_data(self, function_name, params=None):
        response = self.supabase.rpc(function_name, params or {}).execute()
        data = response.data
        df = pd.DataFrame(data)
        return df
        
    def update_supabase(self, supabase_table, df):
        # Convert NaN to None (Supabase can't handle NaN in JSON)
        df = df.replace({np.nan: None})

        # let's add an 'id' column based on the record count
        df['id'] = range(1, len(df) + 1)
        records = df.to_dict(orient='records')
        
        # this is going to add an ID column to track unique transfers. upsert to make sure only new ones are added
        response = (
            self.supabase_insert
            .table(supabase_table)
            .upsert(records, on_conflict='id')
            .execute()
        )

        if not response.data:
            raise Exception(f"Error uploading to table {supabase_table}: {response}")
        
        print(f'✅ {supabase_table} updated successfully!')
        return response.data

class BigQueryClient:
    def __init__(self, project_id: str = None):
        # Load and set credentials
        credentials_path = os.getenv("GOOGLE_CLOUD_KEY")
        if not credentials_path:
            raise ValueError("Missing GOOGLE_CLOUD_KEY in .env")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        self.client = bigquery.Client(project=project_id)

    def create_dataset(self, dataset_id: str, location: str = "US"):
        dataset_ref = bigquery.Dataset(self.client.dataset(dataset_id))
        try:
            self.client.get_dataset(dataset_ref)
            print(f"✅ Dataset '{dataset_id}' already exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.client.create_dataset(dataset)
            print(f"✅ Created dataset '{dataset_id}'.")

    def create_table(self, dataset_id: str, table_id: str, schema: list):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        try:
            self.client.get_table(table_ref)
            print(f"✅ Table '{table_id}' already exists in '{dataset_id}'.")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"✅ Created table '{table_id}' in dataset '{dataset_id}'.")

    def upload_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job = self.client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"📤 Uploaded {df.shape[0]} rows to {dataset_id}.{table_id}")