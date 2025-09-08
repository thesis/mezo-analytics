#!/usr/bin/env python3
"""
Process transactions data from UWI Homes API endpoint.

This script:
1. Fetches transaction data from the API
2. Creates raw and processed DataFrames
3. Uploads raw data to BigQuery raw_data dataset
4. Creates daily aggregations
5. Uploads aggregated data to BigQuery marts dataset
"""

from dotenv import load_dotenv
import pandas as pd
import os
import requests
from datetime import datetime
from mezo.clients import BigQueryClient
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.datetime_utils import format_datetimes

@with_progress("Fetching transactions from API")
def fetch_transactions_data(api_url: str) -> dict:
    """Fetch data from the UWI Homes API endpoint."""
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            raise ValueError("API returned empty response")
            
        return data
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch data from API: {e}")
    except Exception as e:
        raise Exception(f"Error processing API response: {e}")

@with_progress("Processing raw transactions data")
def process_raw_data(api_data: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process the raw API data into DataFrames.
    
    Returns:
        tuple: (raw_df, metadata_df) - Raw transactions and metadata
    """
    if not api_data or 'transactions' not in api_data:
        raise ValueError("Invalid API data structure - missing transactions")
    
    # Extract metadata
    metadata = {
        'dapp_name': api_data.get('dapp_name'),
        'contract_address': api_data.get('contract_address'),
        'period_start': api_data.get('period', {}).get('start_date'),
        'period_end': api_data.get('period', {}).get('end_date'),
        'total_transactions': api_data.get('summary', {}).get('total_transactions'),
        'total_volume': api_data.get('summary', {}).get('total_volume'),
        'total_fees': api_data.get('summary', {}).get('total_fees'),
        'fetch_timestamp': datetime.now().isoformat()
    }
    
    metadata_df = pd.DataFrame([metadata])
    
    # Extract transactions
    transactions = api_data.get('transactions', [])
    if not transactions:
        raise ValueError("No transactions found in API data")
    
    raw_df = pd.json_normalize(transactions)
    
    # Ensure we have required columns
    required_cols = ['transaction_hash', 'timestamp', 'amount', 'transaction_type']
    missing_cols = [col for col in required_cols if col not in raw_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    return raw_df, metadata_df

@with_progress("Cleaning transactions data")
def clean_transactions_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and format the transactions data."""
    if not ExceptionHandler.validate_dataframe(raw_df, "Raw transactions", ['timestamp', 'amount']):
        raise ValueError("Invalid raw transactions data")
    
    df = raw_df.copy()

    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['fee'] = pd.to_numeric(df.get('fee', 0), errors='coerce').fillna(0)
    df['transaction_type'] = df['transaction_type'].str.lower().str.strip()
    df['day_of_week'] = pd.to_datetime(df['date']).dt.day_name()
    df = df.sort_values('date', ascending=False).reset_index(drop=True)
    
    return df

@with_progress("Creating daily aggregations")
def create_daily_aggregations(clean_df: pd.DataFrame) -> pd.DataFrame:
    """Create daily aggregated data."""
    if not ExceptionHandler.validate_dataframe(clean_df, "Clean transactions", ['date', 'amount']):
        raise ValueError("Invalid clean transactions data")
    
    # Group by date and transaction type
    daily_agg = clean_df.groupby(['date', 'transaction_type']).agg({
        'transaction_hash': 'count',  # Count of transactions
        'amount': ['sum', 'mean', 'median'],  # Amount statistics
        'fee': ['sum', 'mean'],  # Fee statistics
        'from': 'nunique',  # Unique senders
        'to': 'nunique'   # Unique receivers
    }).reset_index()
    
    # Flatten column names
    daily_agg.columns = [
        'date', 'transaction_type', 'transaction_count', 
        'total_amount', 'avg_amount', 'median_amount',
        'total_fees', 'avg_fees', 'unique_senders', 'unique_receivers'
    ]
    
    # Pivot to get transaction types as columns
    pivot_df = daily_agg.pivot(
        index='date', 
        columns='transaction_type', 
        values=['transaction_count', 'total_amount', 'total_fees']
    ).reset_index()
    
    # Flatten column names after pivot
    pivot_df.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0] 
        for col in pivot_df.columns
    ]
    
    pivot_df = pivot_df.fillna(0)
    
    # Add total columns
    amount_cols = [col for col in pivot_df.columns if col.startswith('total_amount_')]
    if amount_cols:
        pivot_df['total_volume_all'] = pivot_df[amount_cols].sum(axis=1)
    
    count_cols = [col for col in pivot_df.columns if col.startswith('transaction_count_')]
    if count_cols:
        pivot_df['total_transactions_all'] = pivot_df[count_cols].sum(axis=1)
    
    fee_cols = [col for col in pivot_df.columns if col.startswith('total_fees_')]
    if fee_cols:
        pivot_df['total_fees_all'] = pivot_df[fee_cols].sum(axis=1)
    
    pivot_df['date'] = pivot_df['date'].astype(str)
    pivot_df = pivot_df.sort_values('date', ascending=False).reset_index(drop=True)
    
    return pivot_df

def main():
    """Main function to process UWI Homes transactions data."""
    ProgressIndicators.print_header("UWI HOMES TRANSACTIONS DATA PROCESSING")
    
    try:
        ################################################
        # Setup environment and clients
        ################################################
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        # Initialize BigQuery client
        ProgressIndicators.print_step("Initializing BigQuery", "start")
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
        ProgressIndicators.print_step("BigQuery initialized", "success")

        ################################################
        # Fetch data from UWI API
        ################################################
        api_url = "https://be-mezo-prod.uwihomes.com/api/v1/transactions"
        
        ProgressIndicators.print_step("Fetching data from UWI API", "start")
        api_data = fetch_transactions_data(api_url)
        ProgressIndicators.print_step("API data fetched successfully", "success")

        ################################################
        # Get raw data
        ################################################
        
        ProgressIndicators.print_step("Processing raw data from UWI API", "start")
        raw_transactions_df, metadata_df = process_raw_data(api_data)
        ProgressIndicators.print_step(f"Processed {len(raw_transactions_df)} raw transactions", "success")

        ################################################
        # Upload raw data to BigQuery
        ################################################
        ProgressIndicators.print_step("Uploading raw Uwi data to BigQuery", "start")
        
        if len(raw_transactions_df) > 0:
            raw_transactions_df['transaction_hash'] = raw_transactions_df['transaction_hash'].astype(str)
            bq.update_table(raw_transactions_df, 'raw_data', 'uwi_transactions_raw', 'transaction_hash')
            ProgressIndicators.print_step("Uploaded raw Uwi data to BigQuery", "success")
        
        if len(metadata_df) > 0:
            bq.update_table(metadata_df, 'raw_data', 'uwi_metadata_raw', 'fetch_timestamp')
            ProgressIndicators.print_step("Uploaded metadata to BigQuery", "success")

        ################################################
        # Clean and process data
        ################################################
        clean_transactions_df = clean_transactions_data(raw_transactions_df)
        ProgressIndicators.print_step(f"Cleaned {len(clean_transactions_df)} transactions", "success")

        ################################################
        # Upload cleaned data to BigQuery staging
        ################################################
        ProgressIndicators.print_step("Uploading cleaned Uwi data to BigQuery", "start")

        if len(clean_transactions_df) > 0:
            clean_transactions_df['transaction_hash'] = clean_transactions_df['transaction_hash'].astype(str)
            bq.update_table(clean_transactions_df, 'staging', 'uwi_transactions_clean', 'transaction_hash')
            ProgressIndicators.print_step("Uploaded cleaned Uwi data to BigQuery", "success")

        ################################################
        # Create daily aggregations
        ################################################
        daily_aggregations_df = create_daily_aggregations(clean_transactions_df)
        ProgressIndicators.print_step(f"Created daily aggregations for {len(daily_aggregations_df)} days", "success")

        ################################################
        # Upload processed data to BigQuery marts
        ################################################
        ProgressIndicators.print_step("Uploading daily Uwi data to BigQuery", "start")
        
        if len(daily_aggregations_df) > 0:
            bq.update_table(daily_aggregations_df, 'marts', 'daily_uwi_transactions', 'date')
            ProgressIndicators.print_step("Uploaded daily aggregations to marts", "success")

        ################################################
        # Calculate and display summary statistics
        ################################################
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        
        total_transactions = len(clean_transactions_df)
        total_volume = clean_transactions_df['amount'].sum()
        total_fees = clean_transactions_df['fee'].sum()
        unique_addresses = pd.concat([
            clean_transactions_df['from'], 
            clean_transactions_df['to']
        ]).nunique()
        avg_transaction_size = clean_transactions_df['amount'].mean()
        
        date_range = f"{clean_transactions_df['timestamp'].min()} to {clean_transactions_df['timestamp'].max()}"
        
        ProgressIndicators.print_step("Summary statistics calculated", "success")
        
        # Display sample data for verification
        print(f"\n📄 Sample Raw Data (first 3 rows):")
        print(raw_transactions_df[['transaction_hash', 'timestamp', 'amount', 'transaction_type']].head(3))
        
        print(f"\n📊 Daily Aggregations Summary:")
        print(daily_aggregations_df[['date'] + [col for col in daily_aggregations_df.columns if 'total' in col]].head(3))

        ProgressIndicators.print_summary_box(
            "📊 UWI TRANSACTIONS SUMMARY",
            {
                "Total Transactions": total_transactions,
                "Total Volume": f"{total_volume:,.2f}",
                "Total Fees": f"{total_fees:,.2f}",
                "Unique Addresses": unique_addresses,
                "Average Transaction": f"{avg_transaction_size:,.2f}",
                "Date Range": date_range
            }
        )

        ProgressIndicators.print_header("🚀 UWI HOMES PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header("❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise

if __name__ == "__main__":
    results = main()