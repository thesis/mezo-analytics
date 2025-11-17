from datetime import datetime

from dotenv import load_dotenv
import pandas as pd

from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_config import POOL_TOKEN_PAIRS, POOLS_MAP
from mezo.currency_utils import Conversions
from mezo.datetime_utils import format_datetimes
from mezo.queries import MUSDQueries
from mezo.report_utils import save_metrics_snapshot

# from mezo.test_utils import tests
from mezo.visual_utils import ExceptionHandler, ProgressIndicators, with_progress

################################################
# HELPER FUNCTIONS
################################################

@with_progress("Cleaning swap and fee data")
def clean_swap_and_fee_data(raw):
    """Clean and format swap data with proper token conversions"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw swap data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid input data for cleaning")

    conv = Conversions()
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df = conv.map_pool_to_tokens(df, pool_column='contractId_', pool_token_mapping=POOL_TOKEN_PAIRS)

    amount0_cols = [col for col in df.columns if col.startswith(('amount0'))]
    amount1_cols = [col for col in df.columns if col.startswith(('amount1'))]

    df = conv.format_token_decimals(df, amount_cols=amount0_cols, token_name_col='token0')
    df = conv.format_token_decimals(df, amount_cols=amount1_cols, token_name_col='token1')

    df = conv.add_multi_token_usd_conversions(df, token_configs=[
        {'token_col': 'token0', 'amount_cols': amount0_cols},
        {'token_col': 'token1', 'amount_cols': amount1_cols}
    ])
    
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
def get_daily_swaps_by_pool(df):
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
def get_swaps_by_pool(df):
    """Aggregate swap data by pool, ensuring all pools from POOLS_MAP are included"""
    pool_metrics = df.groupby('pool').agg(
        total_volume=('total_volume', 'sum'),
        total_fees=('total_fees', 'sum'),
        swap_count=('transactionHash_', 'count'),
        users=('user', 'nunique'),
        avg_swap_size=('total_volume', 'mean')
    ).reset_index()
    
    # Ensure all pools from POOLS_MAP are included, even if they have no swaps
    all_pools = set(POOLS_MAP.values())
    existing_pools = set(pool_metrics['pool'].unique())
    missing_pools = all_pools - existing_pools
    
    if missing_pools:
        # Create rows for pools with no swaps (zero values)
        missing_rows = pd.DataFrame({
            'pool': list(missing_pools),
            'total_volume': 0.0,
            'total_fees': 0.0,
            'swap_count': 0,
            'users': 0,
            'avg_swap_size': 0.0
        })
        pool_metrics = pd.concat([pool_metrics, missing_rows], ignore_index=True)
    
    pool_metrics = pool_metrics.sort_values('total_volume', ascending=False)
    
    return pool_metrics

@with_progress("Creating daily time series")
def get_daily_swaps(df):
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

        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.8f}'.format
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        if not skip_bigquery:
            ProgressIndicators.print_step("Initializing database clients", "start")
            bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
            ProgressIndicators.print_step("Database clients initialized", "success")

        # ============================================
        # get raw data + upload to bigquery
        # ============================================

        if not test_mode:
            ProgressIndicators.print_step("Fetching raw swap data from subgraph", "start")
            raw_swap_data = SubgraphClient.get_subgraph_data(
                SubgraphClient.SWAPS_SUBGRAPH, 
                MUSDQueries.GET_SWAPS, 
                'swaps'
            )            
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
        # clean data + upload to bigquery
        # ============================================
        swaps_df_clean = clean_swap_and_fee_data(raw_swap_data)
        fees_df_clean = clean_swap_and_fee_data(raw_fees_data)

        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")

            clean_datasets = [
                (swaps_df_clean, 'stg_swaps_clean', 'transactionHash_'),
                (fees_df_clean, 'stg_swap_fees_clean', 'transactionHash_')
            ]

            print(fees_df_clean['pool'].unique())

            for dataset, table_name, id_column in clean_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'staging', table_name, id_column)
                    ProgressIndicators.print_step(f"✓ Uploaded {table_name}", "success")

        # ============================================
        # merge swaps and fees
        # ============================================
        ProgressIndicators.print_step("Merging swaps with fees", "start")

        swaps_with_fees = pd.merge(
            swaps_df_clean, 
            fees_df_clean, 
            how='left', 
            on='transactionHash_',
            suffixes=('', '_fee')
        )

        print(swaps_with_fees['pool'].unique())

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
        
        keep_cols = ['timestamp', 'pool', 'token0', 'token1', 'amount0In_usd',
       'amount1In_usd', 'amount0Out_usd', 'amount1Out_usd', 'fee0_usd', 'fee1_usd', 
       'user', 'transactionHash_']
        
        int_swaps_with_fees = int_swaps_with_fees[keep_cols]
        
        print(int_swaps_with_fees['fee0_usd'])
        # Fill na rows with missing fee data (if they exist)
        # int_swaps_with_fees = int_swaps_with_fees.fillna(0, subset=['fee0_usd', 'fee1_usd'])
        
        ProgressIndicators.print_step(f"Merged {len(int_swaps_with_fees):,} complete swap records", "success")

        # ============================================
        # get daily and aggregate metrics + upload to bigquery
        # ============================================
        swaps_final = get_daily_swaps_by_pool(int_swaps_with_fees)
        pool_metrics = get_swaps_by_pool(swaps_final)
        daily_metrics = get_daily_swaps(swaps_final)
        pool_daily_metrics = create_swaps_daily_metrics(swaps_final)
        summary_metrics = create_summary_metrics(swaps_final, daily_metrics)

        int_swaps_with_fees.to_csv('int_swaps_with_fees.csv')
        swaps_final.to_csv('swaps_final.csv')
        daily_metrics.to_csv('daily_metrics.csv')
        pool_daily_metrics.to_csv('pool_daily_metrics.csv')
        summary_metrics.to_csv('summary_metrics.csv')

        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading aggregated data to BigQuery", "start")

            analytics_datasets = [
                (int_swaps_with_fees, 'intermediate', 'int_swaps_with_fees', 'transactionHash_'),
                (swaps_final, 'marts', 'm_swaps_with_metrics', 'transactionHash_'),
                (daily_metrics, 'marts', 'm_swap_daily_metrics', 'date'),
                (pool_daily_metrics, 'marts', 'm_swap_pool_daily_metrics', 'date')
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
        # print summary results
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
        top_5_pools = pool_metrics.head()[['pool', 'total_volume', 'swap_count']].copy()
        top_5_pools['total_volume'] = top_5_pools['total_volume'].apply(lambda x: f"${x:,.2f}")
        top_5_pools['swap_count'] = top_5_pools['swap_count'].apply(lambda x: f"{x:,.2f}")
        print(top_5_pools.to_string(index=False))
        print(f"\n{'─' * 60}\n")

        # recalculate summary statistics to ensure they're in the results
        total_swaps = summary_metrics['total_swaps'].iloc[0]
        total_users = summary_metrics['users'].iloc[0]
        total_volume = summary_metrics['total_volume'].iloc[0]
        total_fees = summary_metrics['total_fees'].iloc[0]
        avg_swap_size_usd = summary_metrics['avg_swap_size'].iloc[0]
        
        # calculate 7-day metrics
        seven_day_volume = 0
        seven_day_swaps = 0
        seven_day_users = 0
        seven_day_fees = 0
        
        if len(daily_metrics) > 0:
            recent_swaps = daily_metrics.tail(7)
            seven_day_volume = recent_swaps['daily_volume'].sum()
            seven_day_swaps = recent_swaps['swap_count'].sum()
            seven_day_users = recent_swaps['users'].sum()
            seven_day_fees = recent_swaps['daily_fees'].sum()
        
        # get top pools by volume
        top_pools = []
        if pool_metrics is not None and len(pool_metrics) > 0:
            top_pools = pool_metrics[[
                'pool', 'total_volume', 'swap_count', 'users', 'avg_swap_size'
            ]].to_dict('records')
        
        # create comprehensive metrics dictionary
        metrics_results = {
            'daily_swaps': daily_metrics,
            'daily_swaps_by_pool': pool_daily_metrics,
            'pool_summary': pool_metrics,
            'total_swaps': total_swaps,
            'total_users': total_users,
            'total_volume': total_volume,
            'total_fees': total_fees,
            'avg_swap_size_usd': avg_swap_size_usd,
            'seven_day_volume': seven_day_volume,
            'seven_day_fees': seven_day_fees,
            'seven_day_swaps': seven_day_swaps,
            'seven_day_users': seven_day_users,
            'top_pools': top_pools
        }
        
        # save metrics snapshot for report generation
        save_metrics_snapshot(metrics_results, 'swaps')
        
        ProgressIndicators.print_summary_box(
            f"🔄 SWAPS SUMMARY STATISTICS 🔄",
            {
                "Total Swaps": f"{total_swaps:,.2f}",
                "Unique Users": f"{total_users:,.2f}",
                "Total Volume (USD)": f"${total_volume:,.2f}"
            }
        )

        ProgressIndicators.print_header("🚀 SWAPS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return metrics_results
    
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error: {str(e)}", "error")
        ProgressIndicators.print_header("❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise


if __name__ == "__main__":
    results = main()

    # results = tests.quick_test(main, sample_size=500)
    # tests.inspect_data(results)
    # tests.save_test_outputs(results)