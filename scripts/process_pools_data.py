from dotenv import load_dotenv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import PoolQueries
from mezo.currency_utils import (
    format_pool_token_columns, 
    add_pool_usd_conversions, 
    format_token_columns, 
    add_usd_conversions
)
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import flatten_json_column
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.report_utils import save_metrics_snapshot
from mezo.test_utils import tests
from mezo.currency_config import (
    POOLS_MAP, 
    POOL_TOKEN_PAIRS, 
    TOKENS_ID_MAP, 
    TIGRIS_MAP, 
    MEZO_ASSET_NAMES_MAP
)

# ==================================================
# helper functions
# ==================================================

@with_progress("Calculating the volume for each row")
def get_volume_for_row(row):
    """Determine which volume to use based on token types."""
    volatiles = ['SolvBTC', 'xSolvBTC']
    stables = ['USDC', 'USDT', 'upMUSD']
    
    token0 = row['pool_token0_symbol']
    token1 = row['pool_token1_symbol']
    vol0 = row['totalVolume0_usd']
    vol1 = row['totalVolume1_usd']
    
    # priority 1: MUSD
    if token0 == 'MUSD':
        return vol0
    elif token1 == 'MUSD':
        return vol1
    # priority 2: other stables
    elif token0 in stables and token1 not in stables:
        return vol0
    elif token1 in stables and token0 not in stables:
        return vol1
    # priority 3: take BTC if pair is another volatile asset
    elif token0 == 'BTC' and token1 in volatiles:
        return vol0
    elif token1 == 'BTC' and token0 in volatiles:
        return vol1
    else:
        return max(vol0, vol1)
            
@with_progress("Processing pool deposit/withdrawal data")
def process_pools_data(raw, transaction_type):
    """Process raw data from musd-pools subgraphs"""
    if not ExceptionHandler.validate_dataframe(raw, f"Raw {transaction_type} data", ['contractId_', 'timestamp_']):
        raise ValueError(f"Invalid {transaction_type} data structure")
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['transaction_type'] = transaction_type

    return df

@with_progress("Processing pool volume data")
def process_volume_data(raw_volume):
    """Process raw volume data from Tigris pools"""
    if not ExceptionHandler.validate_dataframe(raw_volume, "Raw volume data", ['timestamp']):
        raise ValueError("Invalid volume data structure")
    
    df = raw_volume.copy()
    
    # Flatten JSON pool column
    df = flatten_json_column(df, 'pool')
    
    # Format timestamps
    df = format_datetimes(df, ['timestamp'])
    
    # Replace token symbols
    token_columns = ['pool_token0_symbol', 'pool_token1_symbol']
    for col in token_columns:
        df[col] = df[col].replace(MEZO_ASSET_NAMES_MAP)
    
    # Map pool names
    df['pool'] = df['pool_name'].map(TIGRIS_MAP)

    # Format token columns
    df = format_token_columns(df, ['totalVolume0'], 'pool_token0_symbol')
    df = format_token_columns(df, ['totalVolume1'], 'pool_token1_symbol')
    
    # Add USD conversions
    df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalVolume0'])
    df = df.drop(columns=['index', 'usd'])
    df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalVolume1'])
    df = df.drop(columns=['index', 'usd'])
    
    return df

@with_progress("Processing pool fees data")
def process_fees_data(raw_fees):
    """Process raw fees data from Tigris pools"""
    if not ExceptionHandler.validate_dataframe(raw_fees, "Raw fees data", ['timestamp']):
        raise ValueError("Invalid fees data structure")
    
    df = raw_fees.copy()
    
    # Flatten JSON pool column
    df = flatten_json_column(df, 'pool')

    # Format timestamps
    df = format_datetimes(df, ['timestamp'])
    
    # Replace token symbols
    token_columns = ['pool_token0_symbol', 'pool_token1_symbol']
    for col in token_columns:
        df[col] = df[col].replace(MEZO_ASSET_NAMES_MAP)

    # Map pool names
    df['pool'] = df['pool_name'].map(TIGRIS_MAP)
    
    # Format token columns
    df = format_token_columns(df, ['totalFees0'], 'pool_token0_symbol')
    df = format_token_columns(df, ['totalFees1'], 'pool_token1_symbol')
    
    # Add USD conversions
    df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalFees0'])
    df = df.drop(columns=['index', 'usd'])
    df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalFees1'])
    df = df.drop(columns=['index', 'usd', 'pool_token0_symbol', 'pool_token1_symbol'])

    df = df[[
        'timestamp', 'pool_name', 'pool', 'totalFees0', 'totalFees1', 
        'totalFees0_usd', 'totalFees1_usd', 'id'
    ]]
    
    return df

@with_progress("Calculating TVL and daily pool metrics")
def calculate_tvl_and_daily_metrics(deposits_df, withdrawals_df):
    """
    Calculate TVL and comprehensive daily metrics for liquidity pools.
    
    Returns:
        tuple: (daily_pool_metrics, daily_pool_metrics_all, tvl_snapshot)
    """
    
    # Combine deposits and withdrawals
    combined = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    combined = combined.sort_values('timestamp_').reset_index(drop=True)

    # Calculate net amounts (deposits positive, withdrawals negative)
    combined['net_amount0_usd'] = np.where(
        combined['transaction_type'] == 'deposit',
        combined['amount0_usd'],
        -combined['amount0_usd']
    )

    combined['net_amount1_usd'] = np.where(
        combined['transaction_type'] == 'deposit',
        combined['amount1_usd'],
        -combined['amount1_usd']
    )

    combined['net_total_usd'] = combined['net_amount0_usd'] + combined['net_amount1_usd']

    # Calculate absolute amounts for tracking
    combined['deposit_amount_usd'] = np.where(
        combined['transaction_type'] == 'deposit',
        combined['amount0_usd'] + combined['amount1_usd'],
        0
    )

    combined['withdrawal_amount_usd'] = np.where(
        combined['transaction_type'] == 'withdrawal',
        combined['amount0_usd'] + combined['amount1_usd'],
        0
    )

    # Calculate cumulative TVL for each pool
    combined['tvl_token0_usd'] = combined.groupby('pool')['net_amount0_usd'].cumsum()
    combined['tvl_token1_usd'] = combined.groupby('pool')['net_amount1_usd'].cumsum()
    combined['tvl_total_usd'] = combined['tvl_token0_usd'] + combined['tvl_token1_usd']

    # Get token information for each pool
    token_info = combined.groupby('pool').agg({
        'token0': 'first',
        'token1': 'first'
    }).to_dict('index')
    
    # =========================================
    # DAILY METRICS BY POOL
    # =========================================
    
    daily_pool_metrics = combined.groupby(['timestamp_', 'pool']).agg({
        # TVL metrics (end of day values)
        'tvl_total_usd': 'last',
        'tvl_token0_usd': 'last',
        'tvl_token1_usd': 'last',
        
        # Flow metrics
        'net_total_usd': 'sum',
        'deposit_amount_usd': 'sum',
        'withdrawal_amount_usd': 'sum',
        
        # Transaction counts
        'transaction_type': 'count',
        
        # Unique users
        'sender': 'nunique'
    }).reset_index()
    
    # Calculate deposit and withdrawal counts
    deposit_counts = combined[combined['transaction_type'] == 'deposit'].groupby(['timestamp_', 'pool']).size()
    withdrawal_counts = combined[combined['transaction_type'] == 'withdrawal'].groupby(['timestamp_', 'pool']).size()
    
    # Add transaction counts to daily metrics
    daily_pool_metrics = daily_pool_metrics.set_index(['timestamp_', 'pool'])
    daily_pool_metrics['deposit_count'] = deposit_counts
    daily_pool_metrics['withdrawal_count'] = withdrawal_counts
    daily_pool_metrics = daily_pool_metrics.fillna(0).reset_index()
    
    # Rename columns for clarity
    daily_pool_metrics.columns = [
        'date', 'pool',
        'tvl_total_usd', 'tvl_token0_usd', 'tvl_token1_usd',
        'daily_net_flow', 'daily_deposits_usd', 'daily_withdrawals_usd',
        'total_transactions', 'unique_users',
        'deposit_transactions', 'withdrawal_transactions'
    ]
    
    # Add token information
    daily_pool_metrics['token0'] = daily_pool_metrics['pool'].map(lambda x: token_info.get(x, {}).get('token0', ''))
    daily_pool_metrics['token1'] = daily_pool_metrics['pool'].map(lambda x: token_info.get(x, {}).get('token1', ''))
    
    # Calculate additional metrics
    daily_pool_metrics['deposit_withdrawal_ratio'] = np.where(
        daily_pool_metrics['daily_withdrawals_usd'] > 0,
        daily_pool_metrics['daily_deposits_usd'] / daily_pool_metrics['daily_withdrawals_usd'],
        np.inf
    )
    
    daily_pool_metrics['tvl_change'] = daily_pool_metrics.groupby('pool')['tvl_total_usd'].diff()
    daily_pool_metrics['tvl_change_pct'] = daily_pool_metrics.groupby('pool')['tvl_total_usd'].pct_change() * 100
    
    # Add 7-day moving averages
    for metric in ['tvl_total_usd', 'daily_deposits_usd', 'daily_withdrawals_usd', 'daily_net_flow']:
        daily_pool_metrics[f'{metric}_ma7'] = daily_pool_metrics.groupby('pool')[metric].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
    
    # =========================================
    # DAILY METRICS FOR ALL POOLS COMBINED  
    # =========================================
    
    daily_pool_metrics_all = daily_pool_metrics.groupby('date').agg({
        # TVL metrics (sum across all pools)
        'tvl_total_usd': 'sum',
        'tvl_token0_usd': 'sum',
        'tvl_token1_usd': 'sum',
        
        # Flow metrics
        'daily_net_flow': 'sum',
        'daily_deposits_usd': 'sum',
        'daily_withdrawals_usd': 'sum',
        
        # Transaction metrics
        'total_transactions': 'sum',
        'deposit_transactions': 'sum',
        'withdrawal_transactions': 'sum',
        
        # User metrics
        'unique_users': 'sum',
        
        # Pool activity
        'pool': 'count'
    }).reset_index()
    
    # Rename columns for clarity
    daily_pool_metrics_all.columns = [
        'date',
        'protocol_tvl_total', 'protocol_tvl_token0', 'protocol_tvl_token1',
        'protocol_daily_net_flow', 'protocol_daily_deposits', 'protocol_daily_withdrawals',
        'protocol_total_transactions', 'protocol_deposit_transactions', 'protocol_withdrawal_transactions',
        'protocol_unique_users', 'active_pools'
    ]
    
    # Calculate protocol-wide additional metrics
    daily_pool_metrics_all['protocol_deposit_withdrawal_ratio'] = np.where(
        daily_pool_metrics_all['protocol_daily_withdrawals'] > 0,
        daily_pool_metrics_all['protocol_daily_deposits'] / daily_pool_metrics_all['protocol_daily_withdrawals'],
        np.inf
    )
    
    daily_pool_metrics_all['protocol_tvl_change'] = daily_pool_metrics_all['protocol_tvl_total'].diff()
    daily_pool_metrics_all['protocol_tvl_change_pct'] = daily_pool_metrics_all['protocol_tvl_total'].pct_change() * 100
    
    # Add 7-day moving averages for protocol metrics
    for metric in ['protocol_tvl_total', 'protocol_daily_deposits', 'protocol_daily_withdrawals', 'protocol_daily_net_flow']:
        daily_pool_metrics_all[f'{metric}_ma7'] = daily_pool_metrics_all[metric].rolling(window=7, min_periods=1).mean()
    
    # =========================================
    # CURRENT TVL SNAPSHOT
    # =========================================
    
    tvl_snapshot = combined.groupby('pool').agg({
        'tvl_total_usd': 'last',
        'tvl_token0_usd': 'last',
        'tvl_token1_usd': 'last',
        'token0': 'first',
        'token1': 'first',
        'timestamp_': ['min', 'max'],
        'transaction_type': 'count',
        'sender': 'nunique'
    }).round(2)
    
    tvl_snapshot.columns = [
        'current_tvl_total', 'current_tvl_token0', 'current_tvl_token1',
        'token0', 'token1', 'first_transaction', 'last_transaction',
        'total_transactions', 'unique_users'
    ]
    
    tvl_snapshot = tvl_snapshot.reset_index()
    
    # Round all numerical values
    daily_pool_metrics = daily_pool_metrics.round(2)
    daily_pool_metrics_all = daily_pool_metrics_all.round(2)
    
    return daily_pool_metrics, daily_pool_metrics_all, tvl_snapshot

@with_progress("Calculating daily volume metrics")
def calculate_volume_metrics(volume_df):
    """
    Calculate daily volume statistics for pools.
    
    Returns:
        tuple: (daily_pool_volume, daily_pool_volume_all)
    """
    
    df = volume_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    
    df = df.sort_values(['pool', 'timestamp'])
    
    df['total_volume'] = df.apply(get_volume_for_row, axis=1)

    df['daily_volume0_usd'] = df['totalVolume0_usd']
    df['daily_volume1_usd'] = df['totalVolume1_usd']
    df['daily_total_volume_usd'] = df['total_volume'] 

    # ===================================
    # DAILY VOLUME BY POOL
    # ===================================
    
    daily_pool_volume = df.groupby(['pool', 'date']).agg({
        'daily_total_volume_usd': 'last',
        'daily_volume0_usd': 'last',
        'daily_volume1_usd': 'last'
    }).reset_index()
    
    # Calculate volume ratio
    daily_pool_volume['volume_ratio'] = np.where(
        daily_pool_volume['daily_volume1_usd'] > 0,
        daily_pool_volume['daily_volume0_usd'] / daily_pool_volume['daily_volume1_usd'],
        np.inf
    )
    
    # add growth metrics
    daily_pool_volume['volume_ma7'] = daily_pool_volume.groupby('pool')['daily_total_volume_usd'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    daily_pool_volume['volume_ma30'] = daily_pool_volume.groupby('pool')['daily_total_volume_usd'].transform(
        lambda x: x.rolling(window=30, min_periods=1).mean()
    )
    daily_pool_volume['volume_growth_rate'] = daily_pool_volume.groupby('pool')['daily_total_volume_usd'].transform(
        lambda x: x.pct_change()
    )
    
    # ===================================
    # DAILY VOLUME FOR ALL POOLS
    # ===================================
    
    daily_pool_volume_all = daily_pool_volume.groupby('date').agg({
        'daily_total_volume_usd': 'sum',
        'daily_volume0_usd': 'sum',
        'daily_volume1_usd': 'sum',
        'pool': 'count'
    }).reset_index()

    daily_pool_volume_all.columns = [
        'date',
        'total_volume_all_pools',
        'total_volume0_all_pools',
        'total_volume1_all_pools',
        'active_pools_count'
    ]

    # Calculate all pool metrics
    daily_pool_volume_all['volume_ratio_all_pools'] = np.where(
        daily_pool_volume_all['total_volume1_all_pools'] > 0,
        daily_pool_volume_all['total_volume0_all_pools'] / daily_pool_volume_all['total_volume1_all_pools'],
        np.inf
    )
    
    daily_pool_volume_all['volume_ma7_all_pools'] = daily_pool_volume_all[
        'total_volume_all_pools'].rolling(
            window=7, min_periods=1
    ).mean()
    
    daily_pool_volume_all['volume_ma30_all_pools'] = daily_pool_volume_all[
        'total_volume_all_pools'].rolling(
            window=30, min_periods=1
    ).mean()
    
    daily_pool_volume_all['volume_growth_rate'] = daily_pool_volume_all[
        'total_volume_all_pools'].pct_change()
    
    daily_pool_volume_all['avg_volume_per_pool'] = (
        daily_pool_volume_all['total_volume_all_pools'] / daily_pool_volume_all['active_pools_count']
    )
    
    daily_pool_volume = daily_pool_volume.round(2)
    daily_pool_volume_all = daily_pool_volume_all.round(2)
    
    return daily_pool_volume, daily_pool_volume_all

@with_progress("Calculating daily fee metrics")
def calculate_fee_metrics(fees_df):
    """
    Calculate daily fee statistics for pools.
    
    Returns:
        tuple: (daily_pool_fees, daily_pool_fees_all)
    """
    
    df = fees_df.copy()

    df = df.sort_values(['pool', 'timestamp'])
    
    df['total_fees_usd'] = df['totalFees0_usd'] + df['totalFees1_usd']
    
    # Daily fees by pool
    daily_pool_fees = df.groupby(['pool', 'timestamp']).agg({
        'totalFees0_usd': 'sum',
        'totalFees1_usd': 'sum',
        'total_fees_usd': 'sum'
    }).reset_index()

    daily_pool_fees_all = daily_pool_fees.groupby('timestamp').agg({
        'total_fees_usd': 'sum',
        'totalFees0_usd': 'sum',
        'totalFees1_usd': 'sum',
        'pool': 'count'
    }).reset_index()
    
    # add 7-day moving averages
    daily_pool_fees_all['fees_ma7'] = daily_pool_fees_all['total_fees_usd'].rolling(
        window=7, min_periods=1).mean()
    daily_pool_fees_all['fees_ma30'] = daily_pool_fees_all['total_fees_usd'].rolling(
        window=30, min_periods=1).mean()
    
    daily_pool_fees_all['fees_growth'] = daily_pool_fees_all[
        'total_fees_usd'].pct_change()
    
    return daily_pool_fees.round(2), daily_pool_fees_all.round(2)

@with_progress("Calculating pool efficiency metrics")
def calculate_efficiency_metrics(tvl_snapshot, daily_volume, daily_fees):
    """Calculate pool efficiency and performance metrics."""
    
    # Get latest TVL for each pool
    tvl_by_pool = tvl_snapshot.set_index('pool')[['current_tvl_total']]
    
    # Get 7-day average volume for each pool
    recent_volume = daily_volume[daily_volume['date'] >= (datetime.now().date() - timedelta(days=7))]
    avg_volume_7d = recent_volume.groupby('pool')['daily_total_volume_usd'].mean()
    
    # Get 7-day average fees for each pool
    recent_fees = daily_fees[daily_fees['timestamp'] >= (datetime.now().date() - timedelta(days=7))]
    avg_fees_7d = recent_fees.groupby('pool')['total_fees_usd'].mean()
    
    # Combine metrics
    efficiency = tvl_by_pool.copy()
    efficiency['avg_daily_volume_7d'] = avg_volume_7d
    efficiency['avg_daily_fees_7d'] = avg_fees_7d
    
    # Calculate efficiency ratios
    efficiency['volume_tvl_ratio'] = efficiency['avg_daily_volume_7d'] / efficiency['current_tvl_total']
    efficiency['capital_efficiency'] = efficiency['volume_tvl_ratio'] * 365  # Annualized turnover
    efficiency['fee_apr'] = (efficiency['avg_daily_fees_7d'] * 365 / efficiency['current_tvl_total']) * 100
    
    # Add risk score based on efficiency metrics
    efficiency['efficiency_score'] = (
        efficiency['capital_efficiency'].rank(pct=True) * 0.4 +
        efficiency['fee_apr'].rank(pct=True) * 0.3 +
        efficiency['volume_tvl_ratio'].rank(pct=True) * 0.3
    ) * 100
    
    efficiency = efficiency.reset_index().round(2)
    
    return efficiency

# ==================================================
# main exe
# ==================================================

def main(test_mode=False, sample_size=False, skip_bigquery=False):
    ProgressIndicators.print_header("POOLS DATA PROCESSING PIPELINE")

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

    # ==========================================================
    # fetch raw data + upload to bigquery
    # ==========================================================
    
        subgraph_data = [
            ('deposits_data', 'pool deposits', SubgraphClient.POOLS_SUBGRAPH, PoolQueries.GET_DEPOSITS, 'mints'),
            ('withdrawals_data', 'pool withdrawals', SubgraphClient.POOLS_SUBGRAPH, PoolQueries.GET_WITHDRAWALS, 'burns'),
            ('volume_data', 'pool volume', SubgraphClient.TIGRIS_POOLS_SUBGRAPH, PoolQueries.GET_POOL_VOLUME, 'poolVolumes'),
            ('fees_data', 'pool fees', SubgraphClient.TIGRIS_POOLS_SUBGRAPH, PoolQueries.GET_TOTAL_POOL_FEES, 'feesStats_collection')
        ]

        data_results = {}

        for var_name, display_name, subgraph, query, query_name in subgraph_data:
            ProgressIndicators.print_step(f"Fetching {display_name} data", "start")
            data_results[var_name] = SubgraphClient.get_subgraph_data(
                subgraph, query, query_name
            )
            ProgressIndicators.print_step(f"Loaded {len(data_results[var_name])} rows of {display_name}", "success")

        deposits_data = data_results['deposits_data']
        withdrawals_data = data_results['withdrawals_data']
        volume_data = data_results['volume_data']
        fees_data = data_results['fees_data']

        if not skip_bigquery:
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

    # ==========================================================
    # clean data + upload to bigquery
    # ==========================================================

        deposits_clean = process_pools_data(deposits_data, 'deposit')
        withdrawals_clean = process_pools_data(withdrawals_data, 'withdrawal')
        volume_clean = process_volume_data(volume_data)
        fees_clean = process_fees_data(fees_data)
        
        if not skip_bigquery:
            ProgressIndicators.print_step("Uploading clean data to BigQuery staging", "start")
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

    # ==========================================================
    # calculate daily and aggregate metrics + upload to bigquery
    # ==========================================================

        daily_pool_tvl, daily_protocol_tvl, tvl_snapshot = calculate_tvl_and_daily_metrics(deposits_clean, withdrawals_clean)
        daily_pool_volume, daily_pool_volume_all = calculate_volume_metrics(volume_clean)
        daily_pool_fees, daily_pool_fees_all = calculate_fee_metrics(fees_clean)
        efficiency_metrics = calculate_efficiency_metrics(tvl_snapshot, daily_pool_volume, daily_pool_fees)

        ProgressIndicators.print_step("Uploading aggregated data to BigQuery marts", "start")
        
        timeseries_datasets = [
            (daily_pool_tvl, 'm_pools_daily_tvl_by_pool', 'date'),
            (daily_protocol_tvl, 'm_pools_daily_tvl', 'date'),
            (daily_pool_volume, 'm_pools_daily_volume_by_pool', 'date'),
            (daily_pool_volume_all, 'm_pools_daily_volume', 'date'),
            (daily_pool_fees, 'm_pools_daily_fees_by_pool', 'timestamp'),
            (daily_pool_fees_all, 'm_pools_daily_fees', 'timestamp')
        ]

        if not skip_bigquery:
            for dataset, table_name, id_column in timeseries_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.update_table(dataset, 'marts', table_name, id_column)
                    ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        snapshot_datasets = [
            (tvl_snapshot, 'm_pools_tvl_snapshot'),
            (efficiency_metrics, 'm_pools_efficiency')
        ]

        for dataset, name in snapshot_datasets:
            dataset.to_csv(f'{name}.csv')
        
        if not skip_bigquery:
            snapshot_datasets = [
                (tvl_snapshot, 'm_pools_tvl_snapshot', 'pool'),
                (efficiency_metrics, 'm_pools_efficiency', 'pool')
            ]

            for dataset, table_name, id_column in snapshot_datasets:
                if dataset is not None and len(dataset) > 0:
                    bq.upsert_table_by_id(dataset, 'marts', table_name, id_column)
                    ProgressIndicators.print_step(f"Upserted {table_name} to BigQuery", "success")

    # ==========================================================
    # display summary stats
    # ==========================================================
        
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        
        total_tvl = tvl_snapshot['current_tvl_total'].sum()
        total_volume_7d = daily_pool_volume[
            daily_pool_volume['date'] >= (datetime.now().date() - timedelta(days=7))
        ]['daily_total_volume_usd'].sum()
        total_fees_7d = daily_pool_fees[
            daily_pool_fees['timestamp'] >= (datetime.now().date() - timedelta(days=7))
        ]['total_fees_usd'].sum()
        
        avg_efficiency = efficiency_metrics['efficiency_score'].mean()
        avg_fee_apr = efficiency_metrics['fee_apr'].mean()
        best_performing_pool = efficiency_metrics.loc[
            efficiency_metrics['efficiency_score'].idxmax(), 'pool'
        ] if len(efficiency_metrics) > 0 else 'N/A'
        
        active_pools = tvl_snapshot[tvl_snapshot['current_tvl_total'] > 0]['pool'].nunique()
        
        ProgressIndicators.print_step("Summary statistics calculated", "success")
        
        ProgressIndicators.print_summary_box(
            "💰 LIQUIDITY POOLS SUMMARY",
            {
                "Total TVL": f"${total_tvl:,.2f}",
                "7-Day Volume": f"${total_volume_7d:,.2f}",
                "7-Day Fees": f"${total_fees_7d:,.2f}",
                "Active Pools": active_pools,
                "Average Fee APR": f"{avg_fee_apr:.2f}%",
                "Average Efficiency Score": f"{avg_efficiency:.1f}/100",
                "Best Performing Pool": best_performing_pool
            }
        )
        
        print("\n📊 Per-Pool TVL Breakdown:")
        print("-" * 50)
        for _, row in tvl_snapshot.iterrows():
            if row['current_tvl_total'] > 0:
                print(f"  {row['pool']:<25} ${row['current_tvl_total']:>15,.2f}")

        # Build the metrics dictionary to return
        metrics_results = {
            'tvl_snapshot': tvl_snapshot,
            'efficiency_metrics': efficiency_metrics,
            'daily_pool_tvl': daily_pool_tvl,
            'daily_protocol_tvl': daily_protocol_tvl,
            'daily_pool_volume': daily_pool_volume,
            'daily_pool_volume_all': daily_pool_volume_all,
            'daily_pool_fees': daily_pool_fees,
            'daily_pool_fees_all': daily_pool_fees_all,
            'total_tvl': total_tvl,
            'active_pools': active_pools,
            'total_volume_7d': total_volume_7d,
            'total_fees_7d': total_fees_7d,
            'avg_efficiency': avg_efficiency,
            'avg_fee_apr': avg_fee_apr,
            'best_performing_pool': best_performing_pool
        }
        
        # Save metrics snapshot for report generation
        save_metrics_snapshot(metrics_results, 'pools')
        
        ProgressIndicators.print_header("🚀 POOLS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return metrics_results
        
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

    # results = tests.quick_test(sample_size=500)
    # tests.inspect_data(results)
    # tests.save_test_outputs(results)