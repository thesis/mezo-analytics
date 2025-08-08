from dotenv import load_dotenv
import pandas as pd
from mezo.currency_utils import format_pool_token_columns, add_pool_usd_conversions, format_token_columns, add_usd_conversions
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, flatten_json_column
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_config import POOLS_MAP, POOL_TOKEN_PAIRS, TOKENS_ID_MAP, TIGRIS_MAP
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.queries import PoolQueries

@with_progress("Processing pool deposits data")
def process_deposits_data(raw_deposits):
    """Process raw deposits data"""
    if not ExceptionHandler.validate_dataframe(raw_deposits, "Raw deposits data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid deposits data structure")
    
    df = raw_deposits.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['count'] = 1
    df['transaction_type'] = 'deposit'
    
    return df


@with_progress("Processing pool withdrawals data")
def process_withdrawals_data(raw_withdrawals):
    """Process raw withdrawals data"""
    if not ExceptionHandler.validate_dataframe(raw_withdrawals, "Raw withdrawals data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid withdrawals data structure")
    
    df = raw_withdrawals.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['count'] = 1
    df['transaction_type'] = 'withdrawal'
    
    return df


@with_progress("Processing pool volume data")
def process_volume_data(raw_volume):
    """Process raw volume data from Tigris pools"""
    if not ExceptionHandler.validate_dataframe(raw_volume, "Raw volume data", ['timestamp']):
        raise ValueError("Invalid volume data structure")
    
    df = raw_volume.copy()
    
    # Flatten JSON pool column
    df = flatten_json_column(df, 'pool')
    
    # Replace token symbols
    token_columns = ['pool_token0_symbol', 'pool_token1_symbol']

    for col in token_columns:
        df[col] = df[col].replace({
            'mUSDC': 'USDC',
            'mUSDT': 'USDT'
        })
    
    # Map pool names
    df['pool'] = df['pool_name'].map(TIGRIS_MAP)
    
    # Format timestamps
    df = format_datetimes(df, ['timestamp'])
    
    # Format token columns
    df = format_token_columns(df, ['totalVolume0'], 'pool_token0_symbol')
    df = format_token_columns(df, ['totalVolume1'], 'pool_token1_symbol')
    
    # Add USD conversions
    df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalVolume0'])
    df = df.drop(columns=['index', 'usd'])
    df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalVolume1'])
    
    df['count'] = 1

    # trim columns
    # df = df[['timestamp', 'totalVolume0', 'totalVolume1', 'pool_name', 'pool', 'totalVolume0_usd', 'totalVolume1_usd', 'count']]
    df = df.drop(columns=['index', 'usd', 'pool_token0_symbol', 'pool_token1_symbol'])
    return df


@with_progress("Processing pool fees data")
def process_fees_data(raw_fees):
    """Process raw fees data from Tigris pools"""
    if not ExceptionHandler.validate_dataframe(raw_fees, "Raw fees data", ['timestamp']):
        raise ValueError("Invalid fees data structure")
    
    df = raw_fees.copy()
    
    # Flatten JSON pool column
    df = flatten_json_column(df, 'pool')
    
    # Replace token symbols
    token_columns = ['pool_token0_symbol', 'pool_token1_symbol']

    for col in token_columns:
        df[col] = df[col].replace({
            'mUSDC': 'USDC',
            'mUSDT': 'USDT'
        })

    # Map pool names
    df['pool'] = df['pool_name'].map(TIGRIS_MAP)
    
    # Format timestamps
    df = format_datetimes(df, ['timestamp'])
    
    # Format token columns
    df = format_token_columns(df, ['totalFees0'], 'pool_token0_symbol')
    df = format_token_columns(df, ['totalFees1'], 'pool_token1_symbol')
    
    # Add USD conversions
    df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalFees0'])
    df = df.drop(columns=['index', 'usd'])
    df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalFees1'])
    
    df['count'] = 1
    df = df.drop(columns=['index', 'usd', 'pool_token0_symbol', 'pool_token1_symbol'])
    
    return df


# @with_progress("Creating daily pools aggregation")
# def create_daily_pools_df(deposits_df, withdrawals_df):
#     """Create daily aggregated pool activity data"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     # Create aggregations
#     daily_pools = combined_df.groupby(['timestamp_']).agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('count', 'sum'),
#         unique_users=('sender', lambda x: x.nunique()),
#         total_amount0=('amount0', 'sum'),
#         total_amount0_usd=('amount0_usd', 'sum'),
#         total_amount1=('amount1', 'sum'),
#         total_amount1_usd=('amount1_usd', 'sum'),
#         net_deposits=('transaction_type', lambda x: (x == 'deposit').sum() - (x == 'withdrawal').sum())
#     ).reset_index()
    
#     # Add total volume
#     daily_pools['total_volume_usd'] = daily_pools['total_amount0_usd'] + daily_pools['total_amount1_usd']
    
#     # Add rolling averages
#     daily_pools = add_rolling_values(daily_pools, 7, ['total_volume_usd', 'total_transactions'])
    
#     return daily_pools


# @with_progress("Creating daily pools by pool aggregation")
# def create_daily_pools_by_pool_df(deposits_df, withdrawals_df):
#     """Create daily pool activity data aggregated by pool"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     # Create aggregations by pool
#     daily_pools_by_pool = combined_df.groupby(['timestamp_', 'pool']).agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('count', 'sum'),
#         unique_users=('sender', lambda x: x.nunique()),
#         total_amount0=('amount0', 'sum'),
#         total_amount0_usd=('amount0_usd', 'sum'),
#         total_amount1=('amount1', 'sum'),
#         total_amount1_usd=('amount1_usd', 'sum'),
#         net_deposits=('transaction_type', lambda x: (x == 'deposit').sum() - (x == 'withdrawal').sum())
#     ).reset_index()
    
#     # Pivot data by pool
#     daily_pools_by_pool_pivot = daily_pools_by_pool.pivot(
#         index='timestamp_', columns='pool'
#     ).fillna(0)
    
#     # Flatten column names
#     daily_pools_by_pool_pivot.columns = [
#         '_'.join(col).strip() for col in daily_pools_by_pool_pivot.columns.values
#     ]
#     daily_pools_by_pool_final = daily_pools_by_pool_pivot.reset_index()
    
#     return daily_pools_by_pool_final


# @with_progress("Creating pool summary statistics")
# def create_pools_summary_df(deposits_df, withdrawals_df):
#     """Create summary statistics by pool"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     pool_summary = combined_df.groupby('pool').agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('count', 'sum'),
#         unique_users=('sender', lambda x: x.nunique()),
#         total_volume_token0=('amount0', 'sum'),
#         total_volume_token0_usd=('amount0_usd', 'sum'),
#         total_volume_token1=('amount1', 'sum'),
#         total_volume_token1_usd=('amount1_usd', 'sum'),
#         avg_transaction_size_usd=('amount0_usd', lambda x: (x + combined_df.loc[x.index, 'amount1_usd']).mean()),
#         net_deposits=('transaction_type', lambda x: (x == 'deposit').sum() - (x == 'withdrawal').sum())
#     ).reset_index()
    
#     # Add total volume
#     pool_summary['total_volume_usd'] = pool_summary['total_volume_token0_usd'] + pool_summary['total_volume_token1_usd']
    
#     return pool_summary


def main():
    """Main function to process pool data."""
    ProgressIndicators.print_header("POOLS DATA PROCESSING PIPELINE")

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

        # Get deposits data
        ProgressIndicators.print_step("Fetching pool deposits data", "start")
        deposits_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.POOLS_SUBGRAPH, 
            PoolQueries.GET_DEPOSITS, 
            'mints'
        )
        
        if not ExceptionHandler.validate_dataframe(
            deposits_data, "Raw deposits data", 
            ['contractId_', 'timestamp_', 'sender', 'amount0', 'amount1']
        ):
            raise ValueError("Invalid deposits data structure")
        
        ProgressIndicators.print_step(f"Loaded {len(deposits_data)} deposit transactions", "success")

        # Get withdrawals data
        ProgressIndicators.print_step("Fetching pool withdrawals data", "start")
        withdrawals_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.POOLS_SUBGRAPH, 
            PoolQueries.GET_WITHDRAWALS, 
            'burns'
        )
        
        if not ExceptionHandler.validate_dataframe(
            withdrawals_data, "Raw withdrawals data", 
            ['contractId_', 'timestamp_', 'sender', 'amount0', 'amount1']
        ):
            raise ValueError("Invalid withdrawals data structure")
        
        ProgressIndicators.print_step(f"Loaded {len(withdrawals_data)} withdrawal transactions", "success")

        # Get volume data
        ProgressIndicators.print_step("Fetching pool volume data", "start")
        volume_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.TIGRIS_POOLS_SUBGRAPH, 
            PoolQueries.GET_POOL_VOLUME, 
            'poolVolumes'
        )
        ProgressIndicators.print_step(f"Loaded {len(volume_data)} volume records", "success")

        # Get fees data
        ProgressIndicators.print_step("Fetching pool fees data", "start")
        fees_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.TIGRIS_POOLS_SUBGRAPH, 
            PoolQueries.GET_TOTAL_POOL_FEES, 
            'feesStats_collection'
        )
        ProgressIndicators.print_step(f"Loaded {len(fees_data)} fee records", "success")

        # Process all data
        deposits_clean = process_deposits_data(deposits_data)
        withdrawals_clean = process_withdrawals_data(withdrawals_data)
        volume_clean = process_volume_data(volume_data)
        fees_clean = process_fees_data(fees_data)

        # Upload raw data to BigQuery
        ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")
        
        fees_data['id'] = fees_data['id'].astype('int')
        volume_data['id'] = volume_data['id'].astype('int')

        raw_datasets = [
            (deposits_data, 'pool_deposits_raw', 'transactionHash_'),
            (withdrawals_data, 'pool_withdrawals_raw', 'transactionHash_'),
            (volume_data, 'pool_volume_raw', 'id'),
            (fees_data, 'pool_fees_raw', 'id')
        ]

        for dataset, table_name, id_column in raw_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'raw_data', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # Upload clean data to BigQuery
        ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
        
        volume_clean['id'] = volume_clean['id'].astype('int')
        fees_clean['id'] = fees_clean['id'].astype('int')

        clean_datasets = [
            (deposits_clean, 'pool_deposits_clean', 'transactionHash_'),
            (withdrawals_clean, 'pool_withdrawals_clean', 'transactionHash_'),
            (volume_clean, 'pool_volume_clean', 'id'),
            (fees_clean, 'pool_fees_clean', 'id')
        ]

        for dataset, table_name, id_column in clean_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'staging', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # Create aggregated datasets
        # daily_pools = create_daily_pools_df(deposits_clean, withdrawals_clean)
        # daily_pools_by_pool = create_daily_pools_by_pool_df(deposits_clean, withdrawals_clean)
        # pool_summary = create_pools_summary_df(deposits_clean, withdrawals_clean)

        # # Upload aggregated data to BigQuery
        # ProgressIndicators.print_step("Uploading aggregated data to BigQuery", "start")
        
        # agg_datasets = [
        #     (daily_pools, 'daily_pools'),
        #     (daily_pools_by_pool, 'daily_pools_by_pool'),
        #     (pool_summary, 'pools_summary')
        # ]

        # for dataset, table_name in agg_datasets:
        #     if dataset is not None and len(dataset) > 0:
        #         if table_name == 'pools_summary':
        #             # Use upsert for summary statistics (updates existing pool rows)
        #             bq.upsert_table(dataset, 'staging', table_name, ['pool'])
        #             ProgressIndicators.print_step(f"Upserted {table_name} to BigQuery", "success")
        #         else:
        #             # Use regular update for time-series data (appends new rows)
        #             dataset_with_id = dataset.copy()
        #             dataset_with_id['id'] = range(1, len(dataset_with_id) + 1)
        #             bq.update_table(dataset_with_id, 'staging', table_name)
        #             ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # Calculate summary statistics
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        
        total_deposits = deposits_clean['count'].sum()
        total_withdrawals = withdrawals_clean['count'].sum()
        total_transactions = total_deposits + total_withdrawals
        total_users = pd.concat([deposits_clean['sender'], withdrawals_clean['sender']]).nunique()
        total_volume_usd = (deposits_clean['amount0_usd'].sum() + deposits_clean['amount1_usd'].sum() + 
                           withdrawals_clean['amount0_usd'].sum() + withdrawals_clean['amount1_usd'].sum())
        active_pools = pd.concat([deposits_clean['pool'], withdrawals_clean['pool']]).nunique()

        ProgressIndicators.print_summary_box(
            "🏊 POOLS SUMMARY STATISTICS 🏊",
            {
                "Total Deposits": total_deposits,
                "Total Withdrawals": total_withdrawals,
                "Total Transactions": total_transactions,
                "Unique Users": total_users,
                "Total Volume (USD)": f"${total_volume_usd:,.2f}",
                "Active Pools": active_pools,
                "Net Deposits": total_deposits - total_withdrawals
            }
        )

        ProgressIndicators.print_header("🚀 POOLS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return {
            'pool_deposits_clean': deposits_clean,
            'pool_withdrawals_clean': withdrawals_clean,
            'pool_volume_clean': volume_clean,
            'pool_fees_clean': fees_clean
        }
        
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