from dotenv import load_dotenv
import pandas as pd

from scripts.get_raw_data import get_all_bridge_transactions
from mezo.currency_utils import format_currency_columns, replace_token_labels
from mezo.currency_config import TOKEN_MAP, TOKEN_TYPE_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import get_token_prices
from mezo.clients import SupabaseClient, BigQueryClient
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress, safe_operation


@with_progress("Cleaning bridge data")
def clean_bridge_data(raw, sort_col, date_cols, currency_cols, asset_col):
    """Clean and format bridge transaction data."""
    if not ExceptionHandler.validate_dataframe(raw, "Raw bridge data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col, ascending=False)
    df = replace_token_labels(df, TOKEN_MAP)
    df = format_datetimes(df, date_cols)
    df = format_currency_columns(df, currency_cols, asset_col)
    df['count'] = 1
    return df


def main():
    """Main function to process bridge transaction data."""
    ProgressIndicators.print_ascii_bridge()
    ProgressIndicators.print_header("BRIDGE DATA PROCESSING PIPELINE")

    try:
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.5f}'.format
        ProgressIndicators.print_step("Environment loaded successfully", "success")
        
        # Get raw bridge transactions
        raw_data = get_all_bridge_transactions()
        
        # Upload raw data to BigQuery
        ProgressIndicators.print_step("Uploading raw bridge data to BigQuery", "start")
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
        if raw_data is not None and len(raw_data) > 0:
            raw_data['id'] = range(1, len(raw_data) + 1)
            bq.update_table(raw_data, 'raw_data', 'bridge_transactions')
            ProgressIndicators.print_step("Uploaded raw bridge data to BigQuery", "success")

        # Load the raw data
        ProgressIndicators.print_step("Loading raw bridge transaction data", "start")
        
        if not ExceptionHandler.validate_dataframe(
            raw_data, "Raw bridge transactions", 
            ['timestamp_', 'amount', 'token', 'recipient']
        ):
            raise ValueError("Invalid raw data structure")
        
        ProgressIndicators.print_step(f"Loaded {len(raw_data)} raw bridge transactions", "success")
        
        # Clean the bridge data
        bridge_txns = clean_bridge_data(raw_data, 'timestamp_', ['timestamp_'], ['amount'], 'token')
        
        # Get token prices from Coingecko API
        ProgressIndicators.print_step("Fetching token prices from Coingecko", "start")
        def fetch_token_prices():
            prices = get_token_prices()
            if prices is None or prices.empty:
                raise ValueError("No token prices received from API")
            return prices
        
        tokens = ExceptionHandler.handle_with_retry(fetch_token_prices, max_retries=3, delay=5.0)
        token_usd_prices = tokens.T.reset_index()
        ProgressIndicators.print_step(f"Retrieved prices for {len(token_usd_prices)} tokens", "success")
        
        # Map token indices and merge with USD prices
        ProgressIndicators.print_step("Processing USD price mappings", "start")
        bridge_txns['index'] = bridge_txns['token'].map(TOKENS_ID_MAP)
        
        # Check for unmapped tokens
        unmapped_tokens = bridge_txns[bridge_txns['index'].isna()]['token'].unique()
        if len(unmapped_tokens) > 0:
            ProgressIndicators.print_step(f"Warning: Unmapped tokens found: {unmapped_tokens}", "warning")
        
        bridge_txns_with_usd = pd.merge(bridge_txns, token_usd_prices, how='left', on='index')
        bridge_txns_with_usd['amount_usd'] = bridge_txns_with_usd['amount'] * bridge_txns_with_usd['usd']
        
        # Check for missing USD prices
        missing_prices = bridge_txns_with_usd[bridge_txns_with_usd['amount_usd'].isna()]
        if len(missing_prices) > 0:
            ProgressIndicators.print_step(f"Warning: {len(missing_prices)} transactions missing USD prices", "warning")
        
        ProgressIndicators.print_step("USD price mapping completed", "success")

        # Remove the 'usd' column before uploading to BigQuery
        bridge_txns_with_usd = bridge_txns_with_usd.drop(columns=['usd'])
        
        # Add id column for BigQuery
        bridge_txns_with_usd['id'] = range(1, len(bridge_txns_with_usd) + 1)

        # Upload bridge transactions with USD to BigQuery staging
        ProgressIndicators.print_step("Uploading bridge transactions with USD to BigQuery staging", "start")
        if bridge_txns_with_usd is not None and len(raw_data) > 0:
            bq.update_table(bridge_txns_with_usd, 'staging', 'bridge_transactions_clean')
        ProgressIndicators.print_step("Uploaded bridge transactions with USD to BigQuery staging", "success")
        
        # Daily bridging data aggregation
        ProgressIndicators.print_step("Creating daily bridge aggregations", "start")
        daily_bridge_txns = bridge_txns_with_usd.groupby(['timestamp_']).agg(
            amount_bridged=('amount_usd', 'sum'),
            users=('recipient', lambda x: x.nunique()),
            transactions=('count', 'sum'),
        ).reset_index()
        ProgressIndicators.print_step(f"Created daily bridge data: {len(daily_bridge_txns)} days", "success")
        
        # Daily bridging data by token
        ProgressIndicators.print_step("Creating daily bridge data by token", "start")
        daily_bridge_txns_by_token = bridge_txns_with_usd.groupby(['timestamp_', 'token']).agg(
            amount_bridged=('amount_usd', 'sum'),
            users=('recipient', lambda x: x.nunique()),
            transactions=('count', 'sum'),
        ).reset_index()
        ProgressIndicators.print_step(f"Created daily bridge data by token: {len(daily_bridge_txns_by_token)} records", "success")
        
        # Pivot the daily data by token
        ProgressIndicators.print_step("Pivoting daily data by token", "start")
        daily_bridge_txns_by_token_pivot = daily_bridge_txns_by_token.pivot(
            index='timestamp_', columns='token'
        ).fillna(0)
        
        # Flatten column names
        daily_bridge_txns_by_token_pivot.columns = [
            '_'.join(col).strip() for col in daily_bridge_txns_by_token_pivot.columns.values
        ]
        daily_bridge_txns_by_token_pivot = daily_bridge_txns_by_token_pivot.reset_index()
        
        # Convert transaction columns to integers
        transactions_cols = [col for col in daily_bridge_txns_by_token_pivot.columns if col.startswith('transactions_')]
        daily_bridge_txns_by_token_final = daily_bridge_txns_by_token_pivot.copy()
        daily_bridge_txns_by_token_final[transactions_cols] = daily_bridge_txns_by_token_final[transactions_cols].astype(int)
        
        ProgressIndicators.print_step(f"Pivot completed - shape: {daily_bridge_txns_by_token_final.shape}", "success")
        
        # Bridge transactions by token summary
        ProgressIndicators.print_step("Creating bridge transactions by token summary", "start")
        bridge_txns_by_token = bridge_txns.groupby('token').agg(
            bridged_amount=('amount', 'sum'),
            bridged_transactions=('count', 'sum')
        ).reset_index()
        
        # Add token type mapping
        bridge_txns_by_token['type'] = bridge_txns_by_token['token'].map(TOKEN_TYPE_MAP)
        
        # Map token indices and merge with USD prices
        bridge_txns_by_token['index'] = bridge_txns_by_token['token'].map(TOKENS_ID_MAP)
        bridge_txns_by_token_with_usd = pd.merge(bridge_txns_by_token, token_usd_prices, how='left', on='index')
        bridge_txns_by_token_with_usd['bridged_amount_usd'] = (
            bridge_txns_by_token_with_usd['bridged_amount'] * bridge_txns_by_token_with_usd['usd']
        )
        
        # Final bridge transactions by token
        final_bridge_txns_by_token = bridge_txns_by_token_with_usd[[
            'token', 'bridged_amount', 'bridged_transactions', 'bridged_amount_usd'
        ]]
        ProgressIndicators.print_step(f"Token summary created: {len(final_bridge_txns_by_token)} tokens", "success")
        
        # Summary statistics
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        total_bridged = bridge_txns_with_usd['amount_usd'].sum()
        total_bridge_txns = bridge_txns_with_usd['count'].sum()
        total_bridgers = bridge_txns_with_usd['recipient'].nunique()
        
        total_bridged_btc_assets = (
            bridge_txns_by_token_with_usd.loc[
                bridge_txns_by_token_with_usd['type'] == 'bitcoin'
            ]['bridged_amount']
        ).sum()
        total_bridged_stablecoins = (
            bridge_txns_by_token_with_usd.loc[
                bridge_txns_by_token_with_usd['type'] == 'stablecoin'
            ]['bridged_amount']
        ).sum()
        total_bridged_T = sum(
            bridge_txns_by_token_with_usd.loc[
                bridge_txns_by_token_with_usd['type'] == 'ethereum'
            ]['bridged_amount']
        )
        
        autobridge_summary = {
            'total_amt_bridged': total_bridged,
            'total_transactions': total_bridge_txns,
            'total_wallets': total_bridgers,
            'total_bitcoin_bridged': total_bridged_btc_assets,
            'total_stablecoins_bridged': total_bridged_stablecoins,
            'total_T_bridged': total_bridged_T
        }
        
        autobridge_summary_df = pd.DataFrame([autobridge_summary])
        ProgressIndicators.print_step("Summary statistics calculated", "success")
        
        ProgressIndicators.print_summary_box(
            f"{ProgressIndicators.COIN} BRIDGE SUMMARY STATISTICS {ProgressIndicators.COIN}",
            {
                "Total Amount Bridged": total_bridged,
                "Total Transactions": total_bridge_txns,
                "Unique Bridgers": total_bridgers,
                "Bitcoin Assets": total_bridged_btc_assets,
                "Stablecoins": total_bridged_stablecoins,
                "T Token": total_bridged_T
            }
        )
        
        # Upload to Supabase with dynamic table creation
        ProgressIndicators.print_step("Uploading to Supabase", "start")
        supabase = SupabaseClient()
        
        # Clean data for Supabase compatibility
        def clean_dataframe_for_upload(df):
            """Clean DataFrame for Supabase upload."""
            import numpy as np
            cleaned_df = df.copy()
            # Convert column names to lowercase
            cleaned_df.columns = [col.lower() for col in df.columns]
            # Convert timestamp to string
            for col in cleaned_df.columns:
                if 'time' in col or 'date' in col or col.endswith('_'):
                    cleaned_df[col] = cleaned_df[col].astype(str)
            # Replace NaN with None
            cleaned_df = cleaned_df.replace({np.nan: None})
            return cleaned_df
        
        upload_operations = [
            ('mainnet_daily_bridge_data', clean_dataframe_for_upload(daily_bridge_txns_by_token_final)),
            ('mainnet_bridge_by_token', clean_dataframe_for_upload(final_bridge_txns_by_token)),
            ('mainnet_bridge_summary', clean_dataframe_for_upload(autobridge_summary_df))
        ]
        
        successful_uploads = 0
        for table_name, data in upload_operations:
            try:
                # Ensure table exists with correct structure
                if supabase.ensure_table_exists_for_dataframe(table_name, data):
                    supabase.update_supabase(table_name, data)
                    ProgressIndicators.print_step(f"Uploaded to {table_name}", "success")
                    successful_uploads += 1
                else:
                    ProgressIndicators.print_step(f"Failed to create/verify table {table_name}", "error")
            except Exception as e:
                ProgressIndicators.print_step(f"Failed to upload to {table_name}: {str(e)}", "error")
        
        if successful_uploads == len(upload_operations):
            ProgressIndicators.print_step("All data uploaded to Supabase successfully", "success")
        else:
            ProgressIndicators.print_step(f"Partial upload success: {successful_uploads}/{len(upload_operations)}", "warning")
        
        ProgressIndicators.print_header(f"{ProgressIndicators.ROCKET} PROCESSING COMPLETED SUCCESSFULLY {ProgressIndicators.ROCKET}")
        
        return {
            'daily_bridge_data': daily_bridge_txns_by_token_final,
            'bridge_by_token': final_bridge_txns_by_token,
            'summary': autobridge_summary_df
        }
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"{ProgressIndicators.ERROR} PROCESSING FAILED")
        print(f"\n{ProgressIndicators.INFO} Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise


if __name__ == "__main__":
    results = main()