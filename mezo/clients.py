import requests
import time
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from web3 import Web3
import json

load_dotenv(dotenv_path='../.env', override=True)

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
    
    def get_subgraph_data(subgraph_url, query, query_key):
        """Generic function to fetch data from subgraphs
        
        Args:
            subgraph_url: The subgraph URL to query
            query: The GraphQL query to execute
            query_key: The key to extract data from the response
        
        Returns:
            pandas.DataFrame: The fetched data as a DataFrame, or None if no data
        """
        musd = SubgraphClient(url=subgraph_url, headers=SubgraphClient.SUBGRAPH_HEADERS)
        
        print(f"🔍 Trying {query_key} query...")
        try:
            data = musd.fetch_subgraph_data(query, query_key)
            if data:
                df = pd.DataFrame(data)
                print(f"✅ Found {len(df)} {query_key} records")
                return df
            else:
                print(f"⚠️ {query_key} query returned no data")
                return None
        except Exception as e:
            print(f"❌ {query_key} query failed: {e}")
            return None
    
    SUBGRAPH_HEADERS = {
        "Content-Type": "application/json",
    }
    
    MEZO_PORTAL_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-portal-mainnet/1.0.0/gn'
    MUSD_MARKET_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/market-mezo/1.0.0/gn'
    MEZO_BRIDGE_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-bridge-mainnet/1.0.0/gn'
    BORROWER_OPS_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/borrower-operations-mezo/1.0.0/gn"
    MUSD_TOKEN_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-token/1.0.0/gn"
    MUSD_STABILITY_POOL_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-stability-pool/1.0.0/gn"
    MUSD_TROVE_MANAGER_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-trove-manager/1.0.0/gn"
    AUGUST_VAULT_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/mezo-vaults-mezo/1.0.0/gn"
    SWAPS_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-pools-mezo/1.0.0/gn"
    POOLS_SUBGRAPH = "https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-pools-mezo/1.0.0/gn"
    TIGRIS_POOLS_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/tigris-pools-mezo/1.0.0/gn'
    WORMHOLE_SUBGRAPH = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/wormhole-bridge-mezo/1.0.0/gn'

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
        """
        DEPRECATED: This method has problematic ID handling.
        Use append_to_supabase() instead for proper incremental uploads.
        
        This method overwrites IDs starting from 1, which can cause conflicts.
        """
        print("⚠️ Warning: update_supabase() has problematic ID handling. Consider using append_to_supabase() instead.")
        
        # Convert NaN to None (Supabase can't handle NaN in JSON)
        df_copy = df.replace({np.nan: None})

        # Add ID column if it doesn't exist - but this approach is problematic
        if 'id' not in df_copy.columns:
            df_copy['id'] = range(1, len(df_copy) + 1)
        
        records = df_copy.to_dict(orient='records')
        
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
    
    def append_to_supabase(self, supabase_table, df):
        """
        Appends new rows to a Supabase table each time it's called.
        Gets the current max ID to avoid conflicts.
        """
        # Convert NaN to None (Supabase can't handle NaN in JSON)
        df = df.replace({np.nan: None})
        
        # Get the current max ID from the table to avoid conflicts
        try:
            response = (
                self.supabase_insert
                .table(supabase_table)
                .select('id')
                .order('id', desc=True)
                .limit(1)
                .execute()
            )
            
            if response.data:
                max_id = response.data[0]['id']
            else:
                max_id = 0
        except Exception as e:
            print(f"Could not fetch max ID, starting from 0: {e}")
            max_id = 0
        
        # Add sequential IDs starting from max_id + 1
        df['id'] = range(max_id + 1, max_id + len(df) + 1)
        records = df.to_dict(orient='records')
        
        # Use insert instead of upsert to always add new rows
        response = (
            self.supabase_insert
            .table(supabase_table)
            .insert(records)
            .execute()
        )

        if not response.data:
            raise Exception(f"Error appending to table {supabase_table}: {response}")
        
        print(f'✅ {len(records)} new rows appended to {supabase_table} successfully!')
        return response.data
 
    def create_dynamic_table_from_dataframe(self, table_name: str, df: pd.DataFrame, 
                                          drop_existing: bool = False,
                                          include_metadata: bool = True,
                                          add_indexes: list = None):
        """
        Dynamically create a table from DataFrame with enhanced features.
        
        Args:
            table_name: Name of the table to create
            df: DataFrame to base the table structure on
            drop_existing: Whether to drop existing table first
            include_metadata: Whether to include created_at/updated_at columns
            add_indexes: List of column names to create indexes on
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from mezo.visual_utils import ProgressIndicators
            ProgressIndicators.print_step(f"Creating table structure for {table_name}", "start")
            
            # Convert column names to lowercase for PostgreSQL compatibility
            lowercase_df = df.copy()
            lowercase_df.columns = [col.lower() for col in df.columns]
            
            # Create a minimal sample row to establish the table structure
            sample_row = {}
            timestamp_columns = []
            
            for col in lowercase_df.columns:
                col_lower = col.lower()
                
                # Detect timestamp/date columns
                is_timestamp = (
                    'time' in col_lower or 
                    'date' in col_lower or 
                    col_lower.endswith('_') or
                    col_lower in ['timestamp', 'created', 'updated']
                )
                
                if is_timestamp:
                    timestamp_columns.append(col)
                    sample_row[col] = '2025-01-01'
                elif lowercase_df[col].dtype == 'object':
                    sample_row[col] = 'sample'
                elif 'int' in str(lowercase_df[col].dtype):
                    sample_row[col] = 0
                elif 'float' in str(lowercase_df[col].dtype):
                    sample_row[col] = 0.0
                else:
                    sample_row[col] = None
            
            sample_df = pd.DataFrame([sample_row])
            
            # Use the existing update_supabase method to create the table
            try:
                self.update_supabase(table_name, sample_df)
                
                # Clear the sample row (try to delete but don't fail if it doesn't exist)
                try:
                    self.supabase_insert.table(table_name).delete().eq('id', 1).execute()
                except:
                    pass  # Ignore if no row to delete
                
                ProgressIndicators.print_step(f"Table {table_name} created successfully", "success")
                
                # Print structure info
                print(f"  • Columns: {len(lowercase_df.columns) + 1}")  # +1 for id
                print(f"  • Timestamp columns detected: {timestamp_columns}")
                if add_indexes:
                    print(f"  • Indexes would be created on: {add_indexes}")
                
                return True
                
            except Exception as e:
                # If table creation fails, it might be because the table doesn't exist in the schema
                # In that case, we'll return False and let the caller handle it
                ProgressIndicators.print_step(f"Table {table_name} needs to be created in Supabase dashboard first", "warning")
                print(f"  • Error: {e}")
                print(f"  • Please create the table manually in Supabase, then retry")
                return False
                
        except Exception as e:
            try:
                from mezo.visual_utils import ProgressIndicators
                ProgressIndicators.print_step(f"Error in dynamic table creation: {e}", "error")
            except:
                print(f"Error in dynamic table creation: {e}")
            return False

    def ensure_table_exists_for_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        Ensure a table exists with the correct structure for the given DataFrame.
        Creates the table if it doesn't exist, or validates structure if it does.
        
        Args:
            table_name: Name of the table
            df: DataFrame to check against
            
        Returns:
            bool: True if table exists and is compatible, False otherwise
        """
        try:
            from mezo.visual_utils import ProgressIndicators
            
            # Try to query the table to see if it exists
            test_query = self.supabase_insert.table(table_name).select('*').limit(1).execute()
            ProgressIndicators.print_step(f"Table {table_name} already exists", "info")
            return True
            
        except Exception:
            # Table doesn't exist, create it
            try:
                from mezo.visual_utils import ProgressIndicators
                ProgressIndicators.print_step(f"Table {table_name} not found, creating it", "start")
            except:
                print(f"Table {table_name} not found, creating it")
            
            # Find timestamp columns for indexing
            timestamp_cols = []
            for col in df.columns:
                col_lower = col.lower()
                if ('time' in col_lower or 'date' in col_lower or 
                    col_lower.endswith('_') or col_lower in ['timestamp', 'created', 'updated']):
                    timestamp_cols.append(col)
            
            return self.create_dynamic_table_from_dataframe(
                table_name=table_name,
                df=df,
                add_indexes=timestamp_cols if timestamp_cols else None
            )

class APIClient:
    """A class to handle API requests for contract data."""

    def __init__(self, base_url, timeout=10):
        self.base_url = base_url
        self.timeout = timeout
    
    def fetch_data(self, endpoint: str) -> pd.DataFrame:
        """Fetch data from the specified API endpoint."""
        
        url = f"{self.base_url}/{self.contract_address}/{endpoint}"
        all_data = []
        next_page_params = None
        
        while True:
            response = requests.get(url, params=next_page_params or {}, timeout=self.timeout)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data: {response.status_code}")

            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            all_data.append(pd.json_normalize(items))
            next_page_params = data.get("next_page_params")
            if not next_page_params:
                break

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()    

class BigQueryClient:
    def __init__(self, key: str = None, project_id: str = None):
        
        # Load and set credentials
        import json
        from google.oauth2 import service_account
        
        credentials_json = os.getenv(key)
        if not credentials_json:
            raise ValueError("Missing {key} in .env")
        
        # Parse JSON credentials
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=[
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        )
        
        self.client = bigquery.Client(project=project_id, credentials=credentials)

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

    def create_table(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        """Create a BigQuery table from a DataFrame."""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
            autodetect=True
        )
        
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()
        print(f"✅ Created table '{table_id}' in dataset '{dataset_id}' with {len(df)} rows.")

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists in BigQuery."""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def upsert_table(self, df: pd.DataFrame, dataset_id: str, table_id: str, key_columns: list):
        """
        Upsert (insert or update) data in BigQuery table.
        Updates existing rows and inserts new ones based on key columns.
        
        Args:
            df: DataFrame to upsert
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID  
            key_columns: List of column names that uniquely identify each row
        """
        # Ensure table exists
        if not self.table_exists(dataset_id, table_id):
            print(f"📋 Table {dataset_id}.{table_id} does not exist. Creating...")
            self.create_table(df, dataset_id, table_id)
            return
        
        print(f"📋 Upserting data to {dataset_id}.{table_id}...")
        
        # Create a temporary table with new data
        temp_table_id = f"{table_id}_temp_{int(time.time())}"
        temp_table_ref = self.client.dataset(dataset_id).table(temp_table_id)
        
        try:
            # Upload new data to temporary table
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True
            )
            
            job = self.client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
            job.result()
            
            # Build the merge query
            key_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
            
            # Get all column names for the update and insert
            columns = list(df.columns)
            if 'id' in columns:
                columns.remove('id')  # Don't update the id column
            
            update_assignments = ", ".join([f"{col} = source.{col}" for col in columns])
            
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])
            
            merge_query = f"""
            MERGE `{dataset_id}.{table_id}` AS target
            USING `{dataset_id}.{temp_table_id}` AS source
            ON {key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
            
            # Execute the merge
            merge_job = self.client.query(merge_query)
            merge_job.result()
            
            print(f"✅ Upserted data to {dataset_id}.{table_id}")
            
        finally:
            # Clean up temporary table
            try:
                self.client.delete_table(temp_table_ref)
            except Exception as e:
                print(f"⚠️ Could not delete temp table: {e}")

    def update_table(self, df: pd.DataFrame, dataset_id: str, table_id: str, id_column: str):
        """
        Update a BigQuery table with new data using an existing ID column for deduplication.
        
        - If table doesn't exist: creates the table
        - If table exists: appends only new rows based on ID column comparison
        
        Args:
            df: DataFrame to upload (must contain the id_column)
            dataset_id: BigQuery dataset ID  
            table_id: BigQuery table ID
            id_column: Name of the existing column to use as unique ID (default: 'transactionHash_')
        """
        # Validate that ID column exists
        if id_column not in df.columns:
            raise ValueError(f"ID column '{id_column}' not found in DataFrame. Available columns: {list(df.columns)}")
        
        print(f"📊 Using column '{id_column}' as unique ID for deduplication")
        
        # Check if table exists
        if not self.table_exists(dataset_id, table_id):
            print(f"📋 Table {dataset_id}.{table_id} does not exist. Creating...")
            self.create_table(df, dataset_id, table_id)
            return
        
        # Table exists - check for existing IDs to avoid duplicates
        print(f"📋 Table {dataset_id}.{table_id} exists. Checking for new rows...")
        
        try:
            # Get existing IDs for deduplication
            existing_ids_query = f"""
            SELECT DISTINCT {id_column}
            FROM `{dataset_id}.{table_id}`
            """
            existing_result = self.client.query(existing_ids_query).to_dataframe()
            
            if existing_result.empty:
                existing_ids = set()
            else:
                existing_ids = set(existing_result[id_column].tolist())
            
            # Filter out rows that already exist
            new_rows_mask = ~df[id_column].isin(existing_ids)
            new_df = df[new_rows_mask].copy()
            
            if new_df.empty:
                print(f"✅ No new rows to upload to {dataset_id}.{table_id}")
                return
            
            print(f"📊 Found {len(new_df)} new rows to upload (filtered out {len(df) - len(new_df)} duplicates)")
            
        except Exception as e:
            error_msg = str(e).lower()
            if f"unrecognized name: {id_column}" in error_msg or f"column {id_column} does not exist" in error_msg:
                print(f"⚠️ Table exists but missing '{id_column}' column. Recreating table...")
                table_ref = self.client.dataset(dataset_id).table(table_id)
                self.client.delete_table(table_ref)
                self.create_table(df, dataset_id, table_id)
                return
            else:
                print(f"❌ Could not check existing data: {e}")
                print(f"❌ Skipping upload to avoid duplicates")
                return
        
        # Upload new rows only
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_dataframe(
            new_df, table_ref, job_config=job_config
        )
        job.result()
        print(f"📤 Successfully uploaded {new_df.shape[0]} new rows to {dataset_id}.{table_id}")

    def upsert_table_by_id(self, df: pd.DataFrame, dataset_id: str, table_id: str, id_column: str):
        """
        Update a BigQuery table with new data using an existing ID column for deduplication.
        
        - If table doesn't exist: creates the table
        - If table exists: updates existing rows and inserts new rows based on ID column
        
        This function provides the same interface as update_table() but performs upserts instead of appends.
        
        Args:
            df: DataFrame to upload (must contain the id_column)
            dataset_id: BigQuery dataset ID  
            table_id: BigQuery table ID
            id_column: Name of the existing column to use as unique ID for upsert operations
        """
        # Validate that ID column exists
        if id_column not in df.columns:
            raise ValueError(f"ID column '{id_column}' not found in DataFrame. Available columns: {list(df.columns)}")
        
        print(f"📊 Using column '{id_column}' as unique ID for upsert operations")
        
        # Check if table exists
        if not self.table_exists(dataset_id, table_id):
            print(f"📋 Table {dataset_id}.{table_id} does not exist. Creating...")
            self.create_table(df, dataset_id, table_id)
            return
        
        # Table exists - use upsert functionality to update existing rows and insert new ones
        print(f"📋 Table {dataset_id}.{table_id} exists. Performing upsert operations...")
        
        try:
            # Use the existing upsert_table method with the id_column as the key
            self.upsert_table(df, dataset_id, table_id, [id_column])
            
        except Exception as e:
            error_msg = str(e).lower()
            if f"unrecognized name: {id_column}" in error_msg or f"column {id_column} does not exist" in error_msg:
                print(f"⚠️ Table exists but missing '{id_column}' column. Recreating table...")
                table_ref = self.client.dataset(dataset_id).table(table_id)
                self.client.delete_table(table_ref)
                self.create_table(df, dataset_id, table_id)
                return
            else:
                print(f"❌ Could not perform upsert operations: {e}")
                print(f"❌ Skipping upload to avoid data corruption")
                return

class Web3Client:
    """A class to handle direct queries to the blockchain"""

    def __init__(self, contract_name: str):
        self.contract_name = contract_name
        self.node = 'https://mainnet.mezo.public.validationcloud.io/'
        self.w3 = Web3(Web3.HTTPProvider(self.node))

    def load_abi(self):
        path_name = f'../mezo/smart_contracts/{self.contract_name}.json'
        with open(path_name, "r") as file:
            content = file.read()
        return json.loads(content)

    def load_contract(self):
        json = self.load_abi()
        address = Web3.to_checksum_address(json['address'])
        return self.w3.eth.contract(address=address, abi=json['abi'])