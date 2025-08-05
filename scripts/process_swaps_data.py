from dotenv import load_dotenv
import pandas as pd
from mezo.currency_utils import format_musd_currency_columns, get_token_prices, format_token_columns
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pool_volume_columns
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import MUSDQueries
from mezo.currency_config import POOLS_MAP, POOL_TOKEN0_MAP, TOKENS_ID_MAP
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress


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

        # Get raw swap data from subgraph
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

        # Upload raw data to BigQuery
        ProgressIndicators.print_step("Uploading raw swap data to BigQuery", "start")
        if raw_swap_data is not None and len(raw_swap_data) > 0:
            raw_swap_data_copy = raw_swap_data.copy()
            raw_swap_data_copy['id'] = range(1, len(raw_swap_data_copy) + 1)
            bq.update_table(raw_swap_data_copy, 'raw_data', 'swaps_raw')
            ProgressIndicators.print_step("Uploaded raw swap data to BigQuery", "success")

        # Clean the swap data
        swaps_df_clean = clean_swap_data(raw_swap_data)

        # Upload cleaned swaps to BigQuery
        ProgressIndicators.print_step("Uploading cleaned swap data to BigQuery", "start")
        if swaps_df_clean is not None and len(swaps_df_clean) > 0:
            swaps_clean_copy = swaps_df_clean.copy()
            swaps_clean_copy['id'] = range(1, len(swaps_clean_copy) + 1)
            bq.update_table(swaps_clean_copy, 'staging', 'swaps_clean')
            ProgressIndicators.print_step("Uploaded clean swap data to BigQuery", "success")

        # Create daily aggregations
        daily_swaps = create_daily_swaps_df(swaps_df_clean)
        daily_swaps_by_pool = create_daily_swaps_by_pool_df(swaps_df_clean)

        # Create pool summary
        pool_summary = create_swaps_by_pool_df(swaps_df_clean)

        # Upload aggregated data to BigQuery
        ProgressIndicators.print_step("Uploading aggregated swap data to BigQuery", "start")
        
        datasets_to_upload = [
            (daily_swaps, 'daily_swaps'),
            (daily_swaps_by_pool, 'daily_swaps_by_pool'),
            (pool_summary, 'swaps_by_pool')
        ]

        for dataset, table_name in datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                if table_name == 'swaps_by_pool':
                    # Use upsert for summary statistics (updates existing pool rows)
                    bq.upsert_table(dataset, 'staging', table_name, ['pool'])
                    ProgressIndicators.print_step(f"Upserted {table_name} to BigQuery", "success")
                else:
                    # Use regular update for time-series data (appends new rows)
                    dataset_with_id = dataset.copy()
                    dataset_with_id['id'] = range(1, len(dataset_with_id) + 1)
                    bq.update_table(dataset_with_id, 'staging', table_name)
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