from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from mezo.currency_utils import format_pool_token_columns, add_usd_conversions
from mezo.datetime_utils import format_datetimes
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import MUSDQueries
from mezo.currency_config import POOL_TOKEN_PAIRS, POOLS_MAP, TOKENS_ID_MAP, MEZO_ASSET_NAMES_MAP
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

################################################
# HELPER FUNCTIONS
################################################

@with_progress("Cleaning swap and fee data")
def clean_swap_and_fee_data(raw, swap=True):
    """Clean and format swap data with proper token conversions"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw swap data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy()
    
    # maps pool contract addresses to readable names
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    
    # changes from UNIX to readable dates
    df = format_datetimes(df, ['timestamp_'])
    
    # converts token amounts from raw values to proper decimals
    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    
    # adds USD conversions for all token amounts with the congecko API

    # make sure to remove "m" from asset names for proper conversions
    token_columns = ['token0', 'token1']    
    for col in token_columns:
        df[col] = df[col].replace(MEZO_ASSET_NAMES_MAP)

    # converts using the coingecko API
    if not swap:
        df = add_usd_conversions(df, 'token0', TOKENS_ID_MAP, ['amount0'])
        df = df.drop(columns=['index', 'usd'])
        df = add_usd_conversions(df, 'token1', TOKENS_ID_MAP, ['amount1'])
        df = df.drop(columns=['index', 'usd'])
    else:
        df = add_usd_conversions(df, 'token0', TOKENS_ID_MAP, ['amount0In', 'amount0Out'])
        df = df.drop(columns=['index', 'usd'])
        df = add_usd_conversions(df, 'token1', TOKENS_ID_MAP, ['amount1In', 'amount1Out'])
        df = df.drop(columns=['index', 'usd'])
    
    df['count'] = 1
    
    return df

def get_swap_volume_for_row(row):
    """
    Calculate swap volume using input side only to avoid double-counting.
    
    Priority logic for which input to use:
    1. MUSD (most stable reference)
    2. Other stablecoins (USDC, USDT, upMUSD)
    3. BTC when paired with volatile BTC derivatives
    4. Otherwise, use the larger of the two inputs
    """
    volatiles = ['SolvBTC', 'xSolvBTC', 'swBTC', 'FBTC', 'tBTC']
    stables = ['USDC', 'USDT', 'upMUSD']
    
    token0 = row['token0']
    token1 = row['token1']
    vol0 = row['amount0In_usd']
    vol1 = row['amount1In_usd']
    
    # Priority 1: MUSD
    if token0 == 'MUSD':
        return vol0
    elif token1 == 'MUSD':
        return vol1
    
    # Priority 2: Other stablecoins
    elif token0 in stables and token1 not in stables:
        return vol0
    elif token1 in stables and token0 not in stables:
        return vol1
    
    # Priority 3: BTC when paired with BTC derivatives
    elif token0 == 'BTC' and token1 in volatiles:
        return vol0
    elif token1 == 'BTC' and token0 in volatiles:
        return vol1
    
    # Default: Use the larger input value
    else:
        return max(vol0, vol1)


@with_progress("Calculating swap metrics")
def calculate_swap_metrics(df):
    """
    Calculate key swap volume metrics:
    - total_volume: Swap volume (input side only)
    - total_fees: Sum of fee amounts in USD
    """
    df = df.sort_values(['pool', 'timestamp'])
    df['total_volume'] = df.apply(get_swap_volume_for_row, axis=1)
    df['total_inflow'] = df['amount0In_usd'] + df['amount1In_usd']
    df['total_outflow'] = df['amount0Out_usd'] + df['amount1Out_usd']
    df['total_fees'] = df['fee0_usd'] + df['fee1_usd']
    
    return df

@with_progress("Aggregating pool-level metrics")
def aggregate_pool_metrics(df):
    """Aggregate swap data by pool"""
    pool_metrics = df.groupby('pool').agg(
        total_volume=('total_volume', 'sum'),
        total_fees=('total_fees', 'sum'),
        swap_count=('transactionHash_', 'count'),
        users=('user', 'nunique'),
        avg_swap_size=('total_volume', 'mean')
    ).reset_index()
    
    pool_metrics = pool_metrics.sort_values('total_volume', ascending=False)
    
    return pool_metrics


@with_progress("Creating daily time series")
def create_daily_timeseries(df):
    """Aggregate swap data by date for time series analysis"""
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df = df.sort_values(['date']).reset_index(drop=True)

    daily_metrics = df.groupby('date').agg(
        daily_volume=('total_volume', 'sum'),
        daily_fees=('total_fees', 'sum'),
        swap_count=('transactionHash_', 'count'),
        users=('user', 'nunique'),
        avg_swap_size=('total_volume', 'mean')
    ).reset_index()
    
    # Add cumulative metrics
    daily_metrics['cumulative_volume'] = daily_metrics['daily_volume'].cumsum()
    daily_metrics['cumulative_fees'] = daily_metrics['daily_fees'].cumsum()
    
    # Add rolling averages (7-day and 30-day)
    daily_metrics['volume_7d_ma'] = daily_metrics['daily_volume'].rolling(window=7, min_periods=1).mean()
    daily_metrics['volume_30d_ma'] = daily_metrics['daily_volume'].rolling(window=30, min_periods=1).mean()
    
    return daily_metrics


@with_progress("Creating pool-date aggregations")
def create_swaps_daily_metrics(df):
    """Aggregate by both pool and date for detailed analysis"""
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    swaps_daily = df.groupby(['date', 'pool']).agg(
        daily_volume=('total_volume', 'sum'),
        daily_fees=('total_fees', 'sum'),
        swap_count=('transactionHash_', 'count'),
        users=('user', 'nunique')
    ).reset_index()
    
    return swaps_daily


@with_progress("Calculating swap summary statistics")
def create_summary_metrics(df, daily_metrics):
    """Create high-level summary statistics"""
    summary = {
        'total_volume': df['total_volume'].sum(),
        'total_fees': df['total_fees'].sum(),
        'total_swaps': len(df),
        'users': df['user'].nunique(),
        'avg_swap_size': df['total_volume'].mean(),
        'median_swap_size': df['total_volume'].median(),
        'total_pools': df['pool'].nunique(),
        'avg_daily_volume': daily_metrics['daily_volume'].mean(),
        'max_daily_volume': daily_metrics['daily_volume'].max(),
        'days_with_activity': len(daily_metrics),
        'updated_on': datetime.today()
    }
    
    summary_df = pd.DataFrame([summary])
    
    return summary_df


################################################
# MAIN PROCESSING PIPELINE
################################################

def main(test_mode=False, sample_size=False, skip_bigquery=False):
    """Main function to process swap data and generate analytics"""
    ProgressIndicators.print_header("SWAPS DATA PROCESSING PIPELINE")

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        if sample_size:
            print(f"{'Using sample size: ' + str(sample_size):^60}")
        if skip_bigquery:
            print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    try:
        # ============================================
        # STEP 1: Setup
        # ============================================
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.8f}'.format
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        if not skip_bigquery:
            ProgressIndicators.print_step("Initializing database clients", "start")
            bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
            ProgressIndicators.print_step("Database clients initialized", "success")

        # ============================================
        # STEP 2: Fetch Raw Data
        # ============================================

        if not test_mode:
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
            
            ProgressIndicators.print_step(f"Loaded {len(raw_swap_data):,} raw swap transactions", "success")

            ProgressIndicators.print_step("Fetching raw swap fees data from subgraph", "start")
            raw_fees_data = SubgraphClient.get_subgraph_data(
                SubgraphClient.SWAPS_SUBGRAPH, 
                MUSDQueries.GET_FEES_FOR_SWAPS,
                'fees'
            )
            ProgressIndicators.print_step(f"Loaded {len(raw_fees_data):,} fee records", "success")

            raw_fees_data.to_csv('raw_fees_data.csv')
            raw_swap_data.to_csv('raw_swap_data.csv')

        else:
            raw_swap_data = pd.read_csv('raw_swap_data.csv')
            raw_fees_data = pd.read_csv('raw_fees_data.csv')

        # ============================================
        # STEP 3: Upload Raw Data to BigQuery
        # ============================================

        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")

            raw_datasets = [
                (raw_swap_data, 'swaps_raw', 'transactionHash_'),
                (raw_fees_data, 'swap_fees_raw', 'transactionHash_')
            ]

            for dataset, table_name, id_column in raw_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'raw_data', table_name, id_column)
                    ProgressIndicators.print_step(f"✓ Uploaded {table_name}", "success")

        # ============================================
        # STEP 4: Clean and Process Data
        # ============================================
        swaps_df_clean = clean_swap_and_fee_data(raw_swap_data)
        fees_df_clean = clean_swap_and_fee_data(raw_fees_data, swap=False)

        # ============================================
        # STEP 5: Upload Clean Data to BigQuery
        # ============================================

        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")

            clean_datasets = [
                (swaps_df_clean, 'swaps_clean', 'transactionHash_'),
                (fees_df_clean, 'swap_fees_clean', 'transactionHash_')
            ]

            for dataset, table_name, id_column in clean_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'staging', table_name, id_column)
                    ProgressIndicators.print_step(f"✓ Uploaded {table_name}", "success")

        # ============================================
        # STEP 6: Merge Swaps with Fees
        # ============================================
        ProgressIndicators.print_step("Merging swaps with fees", "start")

        swaps_with_fees = pd.merge(
            swaps_df_clean, 
            fees_df_clean, 
            how='left', 
            on='transactionHash_',
            suffixes=('', '_fee')
        )

        # Select and rename columns for clarity
        col_map = {
            'timestamp_': 'timestamp',
            'to': 'user',
            'pool': 'pool',
            'token0': 'token0',
            'token1': 'token1',
            'amount0In_usd': 'amount0In_usd',
            'amount1In_usd': 'amount1In_usd',
            'amount0Out_usd': 'amount0Out_usd',
            'amount1Out_usd': 'amount1Out_usd',
            'amount0_usd': 'fee0_usd',
            'amount1_usd': 'fee1_usd',
            'transactionHash_': 'transactionHash_'
        }

        int_swaps_with_fees = swaps_with_fees.rename(columns=col_map)
        
        # Keep only necessary columns
        keep_cols = ['timestamp', 'pool', 'token0', 'token1', 'amount0In_usd',
       'amount1In_usd', 'amount0Out_usd', 'amount1Out_usd', 'fee0_usd', 'fee1_usd', 
       'user', 'transactionHash_']
        
        int_swaps_with_fees = int_swaps_with_fees[keep_cols]
        
        print(int_swaps_with_fees['fee0_usd'])
        # Fill na rows with missing fee data (if they exist)
        # int_swaps_with_fees = int_swaps_with_fees.fillna(0, subset=['fee0_usd', 'fee1_usd'])
        
        ProgressIndicators.print_step(f"Merged {len(int_swaps_with_fees):,} complete swap records", "success")

        # ============================================
        # STEP 7: Calculate Swap Metrics
        # ============================================
        swaps_final = calculate_swap_metrics(int_swaps_with_fees)

        # ============================================
        # STEP 8: Create Aggregated Analytics
        # ============================================
        pool_metrics = aggregate_pool_metrics(swaps_final)
        daily_metrics = create_daily_timeseries(swaps_final)
        pool_daily_metrics = create_swaps_daily_metrics(swaps_final)
        summary_metrics = create_summary_metrics(swaps_final, daily_metrics)

        # ============================================
        # STEP 9: Upload All Data to BigQuery
        # ============================================

        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading aggregated data to BigQuery", "start")

            analytics_datasets = [
                (int_swaps_with_fees, 'intermediate', 'int_swaps_with_fees', 'transactionHash_'),
                (swaps_final, 'marts', 'swaps_with_metrics', 'transactionHash_'),
                (daily_metrics, 'marts', 'swap_daily_metrics', 'date'),
                (pool_daily_metrics, 'marts', 'swap_pool_daily_metrics', 'date')
            ]

            for dataset, schema, table_name, id_column in analytics_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, schema, table_name, id_column)
                    ProgressIndicators.print_step(f"✓ Uploaded {table_name} ({len(dataset):,} rows)", "success")

            upsert_datasets = [
                (pool_metrics, 'marts', 'swap_pool_metrics', ['pool']),
                (summary_metrics, 'marts', 'swap_summary_metrics', ['updated_on'])
            ]

            for dataset, schema, table_name, id_col in upsert_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.upsert_table(dataset, 'marts', table_name, id_col)
                    ProgressIndicators.print_step(f"Upserted {table_name} to BigQuery", "success")

        # ============================================
        # STEP 10: Print Summary Results
        # ============================================
        ProgressIndicators.print_header("📊 SWAP ANALYTICS SUMMARY")
        
        print(f"\n{'─' * 60}")
        print(f"Total Swap Volume:        ${summary_metrics['total_volume'].iloc[0]:,.2f}")
        print(f"Total Fees Generated:     ${summary_metrics['total_fees'].iloc[0]:,.2f}")
        print(f"Total Swaps:              {summary_metrics['total_swaps'].iloc[0]:,}")
        print(f"Unique Traders:           {summary_metrics['users'].iloc[0]:,}")
        print(f"Average Swap Size:        ${summary_metrics['avg_swap_size'].iloc[0]:,.2f}")
        print(f"{'─' * 60}\n")
        
        print("Top 5 Pools by Volume:")
        print(pool_metrics.head()[['pool', 'total_volume', 'swap_count']].to_string(index=False))
        print(f"\n{'─' * 60}\n")

        ProgressIndicators.print_header("🚀 SWAPS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return {
            'swaps_final': swaps_final,
            'pool_metrics': pool_metrics,
            'daily_metrics': daily_metrics,
            'summary_metrics': summary_metrics
        }
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error: {str(e)}", "error")
        ProgressIndicators.print_header("❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise

################################################
# TEST HELPERS
################################################

def quick_test(sample_size=1000):
    """
    Quick test function for development.
    Uses local CSV, samples 1000 rows, skips BigQuery.
    
    Usage:
        from scripts.process_swaps_data import quick_test
        results = quick_test()
        results['pool_metrics']
    """
    return main(test_mode=True, sample_size=sample_size, skip_bigquery=True)


def inspect_data(results, show_head=5):
    """
    Helper function to inspect all output dataframes.
    
    Usage:
        results = quick_test()
        inspect_data(results)
    """
    print(f"\n{'═' * 80}")
    print(f"{'DATA INSPECTION':^80}")
    print(f"{'═' * 80}\n")
    
    for name, df in results.items():
        if isinstance(df, pd.DataFrame):
            print(f"\n{name.upper()}")
            print(f"{'─' * 80}")
            print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            print(f"\nColumns: {', '.join(df.columns.tolist())}")
            print(f"\nFirst {show_head} rows:")
            print(df.head(show_head).to_string())
            print(f"\nData types:")
            print(df.dtypes)
            print(f"\nMemory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            print(f"\n{'─' * 80}\n")


def save_test_outputs(results, output_dir='./test_outputs'):
    """
    Save all test outputs to CSV files for manual inspection.
    
    Usage:
        results = quick_test()
        save_test_outputs(results)
    """
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    
    for name, df in results.items():
        if isinstance(df, pd.DataFrame):
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            print(f"✓ Saved {name} to {filepath}")
    
    print(f"\n✅ All outputs saved to {output_dir}/")

if __name__ == "__main__":
    results = main()

    # For testing, uncomment one of these:
    # results = quick_test(sample_size=500)
    # inspect_data(results)
    # save_test_outputs(results)