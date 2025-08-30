from dotenv import load_dotenv
import pandas as pd
from mezo.currency_utils import add_pool_usd_conversions, format_musd_currency_columns, format_pool_token_columns, get_token_prices, format_token_columns
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pool_volume_columns
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import MUSDQueries
from mezo.currency_config import POOL_TOKEN_PAIRS, POOLS_MAP, POOL_TOKEN0_MAP, TOKENS_ID_MAP
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

################################################
# Define helper functions
################################################

@with_progress("Adding USD conversions to swap data")
def add_usd_conversions_to_swap_data(df):
    """Add USD price conversions to swap data"""
    if not ExceptionHandler.validate_dataframe(df, "Swap data for USD conversion", ['token']):
        raise ValueError("Invalid swap data for USD conversion")
    
    def fetch_token_prices():
        prices = get_token_prices()
        if prices is None or prices.empty:
            raise ValueError("No token prices received from API")
        return prices
    
    tokens = ExceptionHandler.handle_with_retry(fetch_token_prices, max_retries=3, delay=5.0)
    token_usd_prices = tokens.T.reset_index()
    df['index'] = df['token'].map(TOKENS_ID_MAP)

    df_with_usd = pd.merge(df, token_usd_prices, how='left', on='index')
    df_with_usd['amount_usd_in'] = df_with_usd['amount0In'] * df_with_usd['usd']
    df_with_usd['amount_usd_out'] = df_with_usd['amount0Out'] * df_with_usd['usd']

    return df_with_usd


@with_progress("Cleaning swap data")
def clean_swap_data(raw):
    """Clean and format swap data"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw swap data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df['token'] = df['contractId_'].map(POOL_TOKEN0_MAP)
    df = format_token_columns(df, ['amount0In', 'amount0Out'], 'token')
    df = format_musd_currency_columns(df, ['amount1In', 'amount1Out'])
    df['count'] = 1

    df_with_usd = add_usd_conversions_to_swap_data(df)

    return df_with_usd

# @with_progress("Adding USD conversions to fees data")
# def add_usd_conversions_to_fee_data(df):
#     """Add USD price conversions to swap data"""
#     if not ExceptionHandler.validate_dataframe(df, "Swap data for USD conversion", ['token']):
#         raise ValueError("Invalid swap data for USD conversion")
    
#     def fetch_token_prices():
#         prices = get_token_prices()
#         if prices is None or prices.empty:
#             raise ValueError("No token prices received from API")
#         return prices
    
#     tokens = ExceptionHandler.handle_with_retry(fetch_token_prices, max_retries=3, delay=5.0)
#     token_usd_prices = tokens.T.reset_index()
#     df['index'] = df['token'].map(TOKENS_ID_MAP)

#     df_with_usd = pd.merge(df, token_usd_prices, how='left', on='index')
#     df_with_usd['amount_usd_0'] = df_with_usd['amount0'] * df_with_usd['usd']
#     df_with_usd['amount_usd_1'] = df_with_usd['amount1'] * df_with_usd['usd']

#     return df_with_usd

@with_progress("Cleaning fees data")
def clean_fee_data(raw):
    """Clean and format swap fee data"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw fee data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])

    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['count'] = 1

    # df_with_usd = add_usd_conversions_to_fee_data(df)

    return df

@with_progress("Creating daily swap aggregations")
def create_daily_swaps_df(df):
    """Create daily aggregated swap data"""
    daily_df = df.groupby(['timestamp_']).agg(
        total_swaps=('count', 'sum'),
        users=('to', lambda x: x.nunique()),
        first_asset_in=('amount0In', 'sum'),
        first_asset_in_usd=('amount_usd_in', 'sum'),
        first_asset_out=('amount0Out', 'sum'),
        first_asset_out_usd=('amount_usd_out', 'sum'),
        musd_in=('amount1In', 'sum'),
        musd_out=('amount1Out', 'sum')
    ).reset_index()

    daily_df['volume'] = (daily_df['musd_in'] + daily_df['musd_out'])
    daily_df = add_rolling_values(daily_df, 7, ['volume'])
    
    return daily_df


@with_progress("Creating daily swaps by pool")
def create_daily_swaps_by_pool_df(df):
    """Create daily swap data aggregated by pool"""
    daily_swaps_by_pool = df.groupby(['timestamp_', 'pool']).agg(
        total_swaps=('count', 'sum'),
        users=('to', lambda x: x.nunique()),
        first_asset_in=('amount0In', 'sum'),
        first_asset_in_usd=('amount_usd_in', 'sum'),
        first_asset_out=('amount0Out', 'sum'),
        first_asset_out_usd=('amount_usd_out', 'sum'),
        musd_in=('amount1In', 'sum'),
        musd_out=('amount1Out', 'sum')
    ).reset_index()

    # Pivot data by pool
    daily_swaps_by_pool_pivot = daily_swaps_by_pool.pivot(
        index='timestamp_', columns='pool'
    ).fillna(0)

    # Flatten column names
    daily_swaps_by_pool_pivot.columns = [
        '_'.join(col).strip() for col in daily_swaps_by_pool_pivot.columns.values
    ]
    daily_swaps_by_pool_final = daily_swaps_by_pool_pivot.reset_index()

    # Add volume columns for each pool
    daily_swaps_by_pool_with_volume = add_pool_volume_columns(daily_swaps_by_pool_final)

    return daily_swaps_by_pool_with_volume


@with_progress("Creating pool summary statistics")
def create_swaps_by_pool_df(df):
    """Create summary statistics by pool"""
    pool_summary = df.groupby('pool').agg(
        total_swaps=('count', 'sum'),
        unique_users=('to', lambda x: x.nunique()),
        total_volume_musd=('amount1In', 'sum'),
        total_volume_usd=('amount_usd_in', 'sum'),
        avg_swap_size_musd=('amount1In', 'mean'),
        avg_swap_size_usd=('amount_usd_in', 'mean')
    ).reset_index()

    return pool_summary


def main():
    """Main function to process swap data."""
    ProgressIndicators.print_header("SWAPS DATA PROCESSING PIPELINE")

    try:
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.8f}'.format
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        # Initialize clients
        ProgressIndicators.print_step("Initializing database clients", "start")
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
        ProgressIndicators.print_step("Database clients initialized", "success")

        ################################################
        # Get raw data from subgraphs
        ################################################
        
        ProgressIndicators.print_step("Fetching raw swap data from subgraph", "start")
        raw_swap_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.SWAPS_SUBGRAPH, 
            MUSDQueries.GET_SWAPS, 
            'swaps'
        )
        
        if not ExceptionHandler.validate_dataframe(
            raw_swap_data, "Raw swap data", 
            ['contractId_', 'timestamp_', 'to', 'amount0In', 'amount1In']
        ):
            raise ValueError("Invalid raw swap data structure")
        
        ProgressIndicators.print_step(f"Loaded {len(raw_swap_data)} raw swap transactions", "success")

        ProgressIndicators.print_step("Fetching raw swap fees data from subgraph", "start")
        raw_fees_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.SWAPS_SUBGRAPH, 
            MUSDQueries.GET_FEES_FOR_SWAPS,
            'fees'
        )
        ProgressIndicators.print_step(f"Loaded {len(raw_fees_data)} raw swap transactions", "success")
        

        # Upload raw data to BigQuery
        # ProgressIndicators.print_step("Uploading raw swap data to BigQuery", "start")
        # if raw_swap_data is not None and len(raw_swap_data) > 0:
        #     bq.update_table(raw_swap_data, 'raw_data', 'swaps_raw', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded raw swap data to BigQuery", "success")

        ################################################
        # Upload raw data to BigQuery
        ################################################
    
        ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")

        raw_datasets = [
            (raw_fees_data, 'swap_fees_raw', 'transactionHash_'),
            (raw_swap_data, 'swaps_raw', 'transactionHash_')
        ]

        for dataset, table_name, id_column in raw_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'raw_data', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        ################################################
        # Clean and process loan data
        ################################################

        swaps_df_clean = clean_swap_data(raw_swap_data)
        fees_df_clean = clean_fee_data(raw_fees_data)

        # Upload cleaned swaps to BigQuery
        # ProgressIndicators.print_step("Uploading cleaned swap data to BigQuery", "start")
        # if swaps_df_clean is not None and len(swaps_df_clean) > 0:
        #     bq.update_table(swaps_df_clean, 'staging', 'swaps_clean', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded clean swap data to BigQuery", "success")

        ################################################
        # Upload clean and subset dfs to BigQuery
        ################################################

        clean_datasets = [
            (swaps_df_clean, 'swaps_clean', 'transactionHash_'),
            (fees_df_clean, 'swap_fees_clean', 'transactionHash_')
        ]

        for dataset, table_name, id_column in clean_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'staging', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        ################################################
        # Create intermediate swaps data layer
        ################################################

        swaps_with_fees = pd.merge(swaps_df_clean, fees_df_clean, how='left', on='transactionHash_')

        swf_int = swaps_with_fees[['timestamp__x', 'sender_x', 'to', 'contractId__x', 'pool_x', 'pool_y', 
                                                'amount0In', 'amount0Out', 'amount1In', 'amount1Out', 'amount_usd_in', 'amount_usd_out', 
                                                'amount0', 'amount1',  'token0', 'token1', 'amount0_usd', 'amount1_usd', 'transactionHash_']]

        swf_staging = swf_int.dropna(subset=['amount0'])

        col_map = {
            'timestamp__x': 'timestamp', 
            'sender_x': 'from',
            'to': 'to', 
            'contractId__x': 'contractId', 
            'pool_x': 'pool',
            'amount0In': 'amount0_in', 
            'amount0Out': 'amount0_out', 
            'amount1In': 'amount1_in', 
            'amount1Out': 'amount1_out', 
            'amount_usd_in': 'amount_usd_in',
            'amount_usd_out': 'amount_usd_out', 
            'amount0': 'fee0', 
            'amount1': 'fee1',
            'amount0_usd': 'fee0_usd', 
            'amount1_usd': 'fee1_usd', 
            'transactionHash_': 'transactionHash_'
        }

        int_swaps_with_fees = swf_staging.rename(columns=col_map)
        
        ################################################
        # Upload intermediate data to BigQuery
        ################################################

        ProgressIndicators.print_step("Uploading intermediate swap data to BigQuery", "start")
        if int_swaps_with_fees is not None and len(int_swaps_with_fees) > 0:
            bq.update_table(int_swaps_with_fees, 'intermediate', 'int_swaps_with_fees', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded intermediate swap data to BigQuery", "success")

        ################################################
        # Create aggregated swaps data
        ################################################

        # Create daily aggregations
        daily_swaps = create_daily_swaps_df(swaps_df_clean)
        daily_swaps_by_pool = create_daily_swaps_by_pool_df(swaps_df_clean)

        # Create pool summary
        pool_summary = create_swaps_by_pool_df(swaps_df_clean)

        # Upload aggregated data to BigQuery
        ProgressIndicators.print_step("Uploading aggregated swap data to BigQuery", "start")
        
        datasets_to_upload = [
            (daily_swaps, 'agg_daily_swaps', 'timestamp_'),
            (daily_swaps_by_pool, 'agg_daily_swaps_by_pool', 'timestamp_'),
            (pool_summary, 'agg_swaps_by_pool', 'pool')
        ]

        for dataset, table_name, id_col in datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                if table_name == 'swaps_by_pool':
                    # Use upsert for summary statistics (updates existing pool rows)
                    bq.upsert_table(dataset, 'marts', table_name, id_col)
                    ProgressIndicators.print_step(f"Upserted {table_name} to BigQuery", "success")
                else:
                    # Use regular update for time-series data (appends new rows)
                    bq.update_table(dataset, 'marts', table_name, id_col)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")
        
        # # Calculate summary statistics
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        
        total_swaps = swaps_df_clean['count'].sum()
        total_users = swaps_df_clean['to'].nunique()
        total_volume_musd = swaps_df_clean['amount1In'].sum() + swaps_df_clean['amount1Out'].sum()
        total_volume_usd = swaps_df_clean['amount_usd_in'].sum() + swaps_df_clean['amount_usd_out'].sum()
        avg_swap_size_musd = (swaps_df_clean['amount1In'] + swaps_df_clean['amount1Out']).mean()
        

        ProgressIndicators.print_summary_box(
            f"🔄 SWAPS SUMMARY STATISTICS 🔄",
            {
                "Total Swaps": total_swaps,
                "Unique Users": total_users,
                "Total Volume (MUSD)": f"{total_volume_musd:,.2f}",
                "Total Volume (USD)": f"${total_volume_usd:,.2f}",
                "Average Swap Size": f"{avg_swap_size_musd:.2f} MUSD",
                "Active Pools": swaps_df_clean['pool'].nunique()
            }
        )

        ProgressIndicators.print_header(f"🚀 SWAPS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return {
            'daily_swaps': daily_swaps,
            'daily_swaps_by_pool': daily_swaps_by_pool,
            'pool_summary': pool_summary
        }
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise


if __name__ == "__main__":
    results = main()