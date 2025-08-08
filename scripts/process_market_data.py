from dotenv import load_dotenv
import pandas as pd

# from scripts.get_raw_data import get_subgraph_data, get_all_market_donations, get_all_market_purchases
from mezo.clients import SupabaseClient, BigQueryClient, SubgraphClient
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import format_musd_currency_columns
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.currency_config import MUSD_MARKET_MAP
from mezo.queries import MUSDQueries

def replace_market_items(df, col, musd_market_map):
    """
    Replaces values in the specified column using the musd_market_map dictionary.

    Parameters:
        df: The DataFrame containing the column to replace.
        col: The column name to replace values in.
        musd_market_map (dict): Dictionary mapping addresses/IDs to human-readable labels.

    Returns:
        pd.DataFrame: Updated DataFrame with replaced values.
    """
    # Normalize the column to lowercase for matching
    df[col] = df[col].str.lower()

    # Normalize the map keys to lowercase
    normalized_map = {k.lower(): v for k, v in musd_market_map.items()}

    df[col] = df[col].replace(normalized_map)
    
    return df

@with_progress("Processing donations data")
def process_donations_data(donations):
    """Process and format donations data"""
    # Replace recipient addresses with human-readable names
    donations_formatted = replace_market_items(donations, 'recipient', MUSD_MARKET_MAP)
    
    # Format dates and currency amounts
    format_datetimes(donations_formatted, ['timestamp_'])
    format_musd_currency_columns(donations_formatted, ['amount'])
    
    # Rename columns for consistency
    donations_col_map = {
        'timestamp_': 'date', 
        'recipient': 'item',  
        'donor': 'wallet'
    }
    donations_formatted = donations_formatted.rename(columns=donations_col_map)

    return donations_formatted

@with_progress("Processing purchases data")
def process_purchases_data(purchases):
    """Process and format purchases data"""
    # Replace product IDs with human-readable names
    purchases_formatted = replace_market_items(purchases, 'productId', MUSD_MARKET_MAP)
    
    # Format dates and currency amounts
    format_datetimes(purchases_formatted, ['timestamp_'])
    format_musd_currency_columns(purchases_formatted, ['price'])
    
    # Rename columns for consistency
    purchases_col_map = {
        'timestamp_': 'date', 
        'productId': 'item',
        'price': 'amount',
        'customer': 'wallet'
    }
    purchases_formatted = purchases_formatted.rename(columns=purchases_col_map)

    return purchases_formatted

@with_progress("Merging and cleaning market transactions")
def create_market_transactions(donations_formatted, purchases_formatted):
    """Merge donations and purchases data into unified market transactions"""
    # Merge the two datasets
    market_transactions = pd.merge(donations_formatted, purchases_formatted, how='outer').fillna(0)
    
    # Select and clean final columns (ID will be added by BigQuery update_table method)
    market_transactions_final = market_transactions[['date', 'item', 'amount', 'wallet', 'transactionHash_']].copy()
    
    # Convert data types for Supabase compatibility
    market_transactions_final[['date', 'item', 'wallet']] = market_transactions_final[['date', 'item', 'wallet']].astype(str)
    market_transactions_final['amount'] = market_transactions_final['amount'].astype(int)

    market_transactions_final['count'] = 1
    
    return market_transactions_final


def main():
    """Main function to process market transaction data."""
    ProgressIndicators.print_header("MARKET DATA PROCESSING PIPELINE")
    
    try:
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.5f}'.format
        ProgressIndicators.print_step("Environment loaded successfully", "success")
        
        # Get raw market data
        ProgressIndicators.print_step("Fetching market donations data", "start")
        # donations = get_all_market_donations()
        donations = SubgraphClient.get_subgraph_data(
            SubgraphClient.MUSD_MARKET_SUBGRAPH, 
            MUSDQueries.GET_MARKET_DONATIONS,
            'donateds'
        )
        
        ProgressIndicators.print_step("Fetching market purchases data", "start")
        # purchases = get_all_market_purchases()
        purchases = SubgraphClient.get_subgraph_data(
            SubgraphClient.MUSD_MARKET_SUBGRAPH, 
            MUSDQueries.GET_MARKET_PURCHASES,
            'orderPlaceds'
        )
        
        # Upload raw data to BigQuery
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

        ProgressIndicators.print_step("Uploading raw market data to BigQuery", "start")
        if donations is not None and len(donations) > 0:
            bq.update_table(donations, 'raw_data', 'market_donations_raw', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded raw donations to BigQuery", "success")
            
        if purchases is not None and len(purchases) > 0:
            bq.update_table(purchases, 'raw_data', 'market_purchases_raw', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded raw purchases to BigQuery", "success")
        
        # Validate raw data
        if not ExceptionHandler.validate_dataframe(
            donations, "Market donations", 
            ['timestamp_', 'recipient', 'amount', 'donor']
        ):
            raise ValueError("Invalid donations data structure")
            
        if not ExceptionHandler.validate_dataframe(
            purchases, "Market purchases", 
            ['timestamp_', 'productId', 'price', 'customer']
        ):
            raise ValueError("Invalid purchases data structure")
        
        ProgressIndicators.print_step(f"Loaded {len(donations)} donations and {len(purchases)} purchases", "success")
        
        # Process the data
        donations_processed = process_donations_data(donations)
        purchases_processed = process_purchases_data(purchases)
        
        # Create unified market transactions
        market_transactions_final = create_market_transactions(donations_processed, purchases_processed)

        ProgressIndicators.print_step("Uploading cleaned market data to BigQuery", "start")
        if market_transactions_final is not None and len(market_transactions_final) > 0:
            # Use transaction hash as unique ID for deduplication (now the default)
            bq.update_table(market_transactions_final, 'staging', 'market_transactions_clean', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded cleaned market data to BigQuery", "success")
        
        # Display summary statistics
        ProgressIndicators.print_summary_box(
            f"{ProgressIndicators.COIN} MARKET TRANSACTION SUMMARY {ProgressIndicators.COIN}",
            {
                "Total Transactions": len(market_transactions_final),
                "Total Amount": market_transactions_final['amount'].sum(),
                "Unique Items": market_transactions_final['item'].nunique(),
                "Unique Wallets": market_transactions_final['wallet'].nunique(),
                "Date Range": f"{market_transactions_final['date'].min()} to {market_transactions_final['date'].max()}"
            }
        )
        
        # Upload to Supabase
        # ProgressIndicators.print_step("Uploading to Supabase", "start")
        # supabase = SupabaseClient()
        
        # try:
        #     # Ensure table exists with correct structure
        #     if supabase.ensure_table_exists_for_dataframe('mainnet_musd_market_txns', market_transactions_final):
        #         supabase.update_supabase('mainnet_musd_market_txns', market_transactions_final)
        #         ProgressIndicators.print_step("Data uploaded to Supabase successfully", "success")
        #     else:
        #         ProgressIndicators.print_step("Failed to create/verify table mainnet_musd_market_txns", "error")
        # except Exception as e:
        #     ProgressIndicators.print_step(f"Failed to upload to Supabase: {str(e)}", "error")
        #     raise
        
        # ProgressIndicators.print_header(f"{ProgressIndicators.ROCKET} MARKET DATA PROCESSING COMPLETED SUCCESSFULLY {ProgressIndicators.ROCKET}")
        
        # return market_transactions_final
        
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