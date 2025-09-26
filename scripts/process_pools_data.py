from dotenv import load_dotenv
import pandas as pd
import numpy as np
from mezo.currency_utils import format_pool_token_columns, add_pool_usd_conversions, format_token_columns, add_usd_conversions
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, flatten_json_column
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_config import POOLS_MAP, POOL_TOKEN_PAIRS, TOKENS_ID_MAP, TIGRIS_MAP
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.queries import PoolQueries

@with_progress("Processing pool data")
def process_pools_data(raw, transaction_type):
    """Process raw data from musd-pools subgraphs"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid data structure")
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])
    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['transaction_type'] = transaction_type

    return df

@with_progress("Calculating TVL and daily aggregates")
def agg_pools_data(deposits_df, withdrawals_df):
    """
    Create comprehensive daily metrics for liquidity pools including deposits, 
    withdrawals, transactions, and TVL for both individual pools and protocol-wide.
    
    Parameters:
    -----------
    deposits_df : pandas.DataFrame
        DataFrame with deposit transactions
    withdrawals_df : pandas.DataFrame
        DataFrame with withdrawal transactions
    
    Returns:
    --------
    tuple: (daily_pool_metrics, daily_protocol_metrics, tvl_snapshot)
        - daily_pool_metrics: Daily metrics for each pool
        - daily_protocol_metrics: Daily metrics for all pools combined
        - tvl_snapshot: Current TVL snapshot for each pool
    """
    
    # Ensure we're working with copies
    deposits = deposits_df.copy()
    withdrawals = withdrawals_df.copy()
    
    # Combine deposits and withdrawals
    combined = pd.concat([deposits, withdrawals], ignore_index=True)
        
    # Sort by timestamp for proper cumulative calculations
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

    # Calculate absolute amounts for deposits/withdrawals tracking
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
    # PART 1: DAILY METRICS BY POOL
    # =========================================
    
    daily_pool_metrics = combined.groupby(['timestamp_', 'pool']).agg({
        # TVL metrics (end of day values)
        'tvl_total_usd': 'last',
        'tvl_token0_usd': 'last',
        'tvl_token1_usd': 'last',
        
        # Flow metrics
        'net_total_usd': 'sum',  # Net daily flow
        
        # Deposit metrics
        'deposit_amount_usd': 'sum',  # Total daily deposits
        
        # Withdrawal metrics  
        'withdrawal_amount_usd': 'sum',  # Total daily withdrawals
        
        # Transaction counts
        'transaction_type': 'count',  # Total transactions
        
        # Unique users
        'sender': 'nunique'  # Unique addresses
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
        'timestamp_', 'pool',
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
    
    # Calculate 7-day moving averages for key metrics
    for metric in ['tvl_total_usd', 'daily_deposits_usd', 'daily_withdrawals_usd', 'daily_net_flow']:
        daily_pool_metrics[f'{metric}_7d_ma'] = daily_pool_metrics.groupby('pool')[metric].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
    
    # =========================================
    # PART 2: DAILY METRICS FOR ALL POOLS COMBINED
    # =========================================
    
    daily_protocol_metrics = daily_pool_metrics.groupby('timestamp_').agg({
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
        'unique_users': 'sum',  # Note: might count same user across pools
        
        # Pool activity
        'pool': 'count'  # Number of active pools
    }).reset_index()
    
    # Rename columns for clarity
    daily_protocol_metrics.columns = [
        'timestamp_',
        'protocol_tvl_total', 'protocol_tvl_token0', 'protocol_tvl_token1',
        'protocol_daily_net_flow', 'protocol_daily_deposits', 'protocol_daily_withdrawals',
        'protocol_total_transactions', 'protocol_deposit_transactions', 'protocol_withdrawal_transactions',
        'protocol_unique_users', 'active_pools'
    ]
    
    # Calculate protocol-wide additional metrics
    daily_protocol_metrics['protocol_deposit_withdrawal_ratio'] = np.where(
        daily_protocol_metrics['protocol_daily_withdrawals'] > 0,
        daily_protocol_metrics['protocol_daily_deposits'] / daily_protocol_metrics['protocol_daily_withdrawals'],
        np.inf
    )
    
    daily_protocol_metrics['protocol_tvl_change'] = daily_protocol_metrics['protocol_tvl_total'].diff()
    daily_protocol_metrics['protocol_tvl_change_pct'] = daily_protocol_metrics['protocol_tvl_total'].pct_change() * 100
    
    # Calculate 7-day moving averages for protocol metrics
    for metric in ['protocol_tvl_total', 'protocol_daily_deposits', 'protocol_daily_withdrawals', 'protocol_daily_net_flow']:
        daily_protocol_metrics[f'{metric}_7d_ma'] = daily_protocol_metrics[metric].rolling(window=7, min_periods=1).mean()
    
    # Calculate average metrics per pool
    daily_protocol_metrics['avg_tvl_per_pool'] = daily_protocol_metrics['protocol_tvl_total'] / daily_protocol_metrics['active_pools']
    daily_protocol_metrics['avg_deposits_per_pool'] = daily_protocol_metrics['protocol_daily_deposits'] / daily_protocol_metrics['active_pools']
    daily_protocol_metrics['avg_withdrawals_per_pool'] = daily_protocol_metrics['protocol_daily_withdrawals'] / daily_protocol_metrics['active_pools']
    
    # Identify high activity days
    daily_protocol_metrics['high_deposit_day'] = daily_protocol_metrics['protocol_daily_deposits'] > daily_protocol_metrics['protocol_daily_deposits'].quantile(0.9)
    daily_protocol_metrics['high_withdrawal_day'] = daily_protocol_metrics['protocol_daily_withdrawals'] > daily_protocol_metrics['protocol_daily_withdrawals'].quantile(0.9)
    
    # =========================================
    # PART 3: CURRENT TVL SNAPSHOT
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
    
    # Round all numerical values for cleaner output
    daily_pool_metrics = daily_pool_metrics.round(2)
    daily_protocol_metrics = daily_protocol_metrics.round(2)
    
    return daily_pool_metrics, daily_protocol_metrics, tvl_snapshot

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
        df[col] = df[col].replace({
            'mUSDC': 'USDC',
            'mUSDT': 'USDT',
            'mSolvBTC': 'SolvBTC',
            'mxSolvBTC': 'xSolvBTC'
        })
    
    # Map pool names
    df['pool'] = df['pool_name'].map(TIGRIS_MAP)

    # Format token columns
    df = format_token_columns(df, ['totalVolume0'], 'pool_token0_symbol')
    df = format_token_columns(df, ['totalVolume1'], 'pool_token1_symbol')
    
    # Add USD conversions
    df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalVolume0'])
    df = df.drop(columns=['index', 'usd'])
    df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalVolume1'])
    df = df.drop(columns=['index', 'usd', 'pool_token0_symbol', 'pool_token1_symbol'])

    # Trim dataframe
    df = df[[
        'timestamp', 'pool_name', 'pool', 
        'totalVolume0', 'totalVolume1', 
        'totalVolume0_usd', 'totalVolume1_usd', 'id'
    ]]

    return df

@with_progress("Analyzing volume data by pool")
def analyze_pool_volumes(df):
    """
    Analyze liquidity pool volume data and calculate key metrics.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with columns: timestamp, pool_name, totalVolume0_usd, totalVolume1_usd
    
    Returns:
    --------
    pandas.DataFrame with additional calculated metrics
    """
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort by pool and timestamp for proper calculations
    df = df.sort_values(['pool_name', 'timestamp'])
    
    # Calculate total pool volume (cumulative)
    df['total_pool_volume_usd'] = df['totalVolume0_usd'] + df['totalVolume1_usd']
    
    # Calculate daily volumes (difference from previous day for each pool)
    df['daily_volume0_usd'] = df.groupby('pool_name')['totalVolume0_usd'].diff()
    df['daily_volume1_usd'] = df.groupby('pool_name')['totalVolume1_usd'].diff()
    df['daily_pool_volume_usd'] = df['daily_volume0_usd'].fillna(0) + df['daily_volume1_usd'].fillna(0)
    
    # For first entry of each pool, use total as daily (no previous data)
    first_entries = df.groupby('pool_name').first().index
    mask = df.set_index('pool_name').index.isin(first_entries)
    df.loc[mask & df['daily_pool_volume_usd'].isna(), 'daily_pool_volume_usd'] = \
        df.loc[mask & df['daily_pool_volume_usd'].isna(), 'total_pool_volume_usd']
    
    # Calculate volume ratios (helps identify which token is traded more)
    df['volume_ratio'] = np.where(
        df['totalVolume1_usd'] > 0,
        df['totalVolume0_usd'] / df['totalVolume1_usd'],
        np.inf
    )
    
    # Calculate 7-day moving average of daily volume
    df['volume_7d_ma'] = df.groupby('pool_name')['daily_pool_volume_usd'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    
    # Calculate volume growth rate
    df['volume_growth_rate'] = df.groupby('pool_name')['daily_pool_volume_usd'].transform(
        lambda x: x.pct_change()
    )
    
    # Mark significant volume days (> 90th percentile for each pool)
    df['is_high_volume'] = df.groupby('pool_name')['daily_pool_volume_usd'].transform(
        lambda x: x > x.quantile(0.9)
    )
    
    return df

@with_progress("Aggregating daily volume data")
def agg_volume_data(volume_df):
    """
    Create a comprehensive daily volume statistics DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame with columns: timestamp, pool_name, totalVolume0_usd, totalVolume1_usd
        
    Returns:
    --------
    tuple: (daily_pool_stats, daily_protocol_stats)
        - daily_pool_stats: Daily statistics for each pool
        - daily_protocol_stats: Daily statistics for all pools combined
    """
    
    # Make a copy to avoid modifying original
    df = volume_df.copy()
    
    # Ensure timestamp is datetime and create date column
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].dt.date
    
    # Sort by pool and timestamp for proper calculations
    df = df.sort_values(['pool_name', 'timestamp'])
    
    # Calculate total cumulative volume for each record
    df['total_volume_usd'] = df['totalVolume0_usd'] + df['totalVolume1_usd']
    
    # Calculate daily volume (difference from previous day for each pool)
    df['daily_volume0_usd'] = df.groupby('pool_name')['totalVolume0_usd'].diff()
    df['daily_volume1_usd'] = df.groupby('pool_name')['totalVolume1_usd'].diff()
    df['daily_total_volume_usd'] = df['daily_volume0_usd'].fillna(0) + df['daily_volume1_usd'].fillna(0)
    
    # For first entry of each pool, use total as daily
    first_entries = df.groupby('pool_name').first().index
    mask = df.set_index('pool_name').index.isin(first_entries)
    df.loc[mask & df['daily_total_volume_usd'].isna(), 'daily_total_volume_usd'] = \
        df.loc[mask & df['daily_total_volume_usd'].isna(), 'total_volume_usd']
    
    # ===================================
    # PART 1: DAILY STATS BY POOL
    # ===================================
    
    daily_pool_stats = df.groupby(['timestamp', 'pool_name']).agg({
        'daily_total_volume_usd': 'last',  # Daily volume for the pool
        'daily_volume0_usd': 'last',       # Token0 daily volume
        'daily_volume1_usd': 'last'         # Token1 daily volume
    }).reset_index()
    
    # Calculate volume ratio (Token0 / Token1)
    daily_pool_stats['volume_ratio'] = np.where(
        daily_pool_stats['daily_volume1_usd'] > 0,
        daily_pool_stats['daily_volume0_usd'] / daily_pool_stats['daily_volume1_usd'],
        np.inf
    )
    
    # Calculate 7-day moving average for each pool
    daily_pool_stats['volume_7d_ma'] = daily_pool_stats.groupby('pool_name')['daily_total_volume_usd'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )
    
    # Calculate growth rate (day-over-day percentage change)
    daily_pool_stats['growth_rate'] = daily_pool_stats.groupby('pool_name')['daily_total_volume_usd'].transform(
        lambda x: x.pct_change() * 100
    )
    
    # Identify significant volume days (> 90th percentile for each pool)
    daily_pool_stats['is_significant_volume'] = daily_pool_stats.groupby('pool_name')['daily_total_volume_usd'].transform(
        lambda x: x > x.quantile(0.9)
    )
    
    # Add pool rank by daily volume
    daily_pool_stats['daily_rank'] = daily_pool_stats.groupby('timestamp')['daily_total_volume_usd'].rank(
        method='dense', ascending=False
    )
    
    # Round numerical values for cleaner output
    daily_pool_stats = daily_pool_stats.round({
        'daily_total_volume_usd': 2,
        'daily_volume0_usd': 2,
        'daily_volume1_usd': 2,
        'volume_ratio': 3,
        'volume_7d_ma': 2,
        'growth_rate': 2
    })
    
    # ===================================
    # PART 2: DAILY STATS FOR ALL POOLS COMBINED
    # ===================================
    
    daily_protocol_stats = daily_pool_stats.groupby('timestamp').agg({
        'daily_total_volume_usd': 'sum',      # Total volume across all pools
        'daily_volume0_usd': 'sum',           # Total Token0 volume
        'daily_volume1_usd': 'sum',           # Total Token1 volume
        'is_significant_volume': 'sum',       # Count of pools with significant volume
        'pool_name': 'count'                  # Number of active pools
    }).reset_index()

    # Rename columns for clarity
    daily_protocol_stats.columns = [
        'timestamp',
        'total_volume_all_pools',
        'total_volume0_all_pools',
        'total_volume1_all_pools',
        'pools_with_significant_volume',
        'active_pools_count'
    ]

    # Calculate protocol-wide volume ratio
    daily_protocol_stats['volume_ratio_all_pools'] = np.where(
        daily_protocol_stats['total_volume1_all_pools'] > 0,
        daily_protocol_stats['total_volume0_all_pools'] / daily_protocol_stats['total_volume1_all_pools'],
        np.inf
    )
    
    # Calculate 7-day moving average for protocol
    daily_protocol_stats['volume_7d_ma_all_pools'] = daily_protocol_stats['total_volume_all_pools'].rolling(
        window=7, min_periods=1
    ).mean()
    
    # Calculate protocol-wide growth rate
    daily_protocol_stats['growth_rate_all_pools'] = daily_protocol_stats['total_volume_all_pools'].pct_change() * 100

    # Identify significant volume days for protocol (> 90th percentile)
    threshold = daily_protocol_stats['total_volume_all_pools'].quantile(0.9)
    daily_protocol_stats['is_significant_volume_day'] = daily_protocol_stats['total_volume_all_pools'] > threshold

    # Add some additional protocol metrics
    daily_protocol_stats['avg_volume_per_pool'] = (
        daily_protocol_stats['total_volume_all_pools'] / daily_protocol_stats['active_pools_count']
    )

    # Calculate concentration (what % of volume came from top pool)
    top_pool_volumes = daily_pool_stats.groupby('timestamp')['daily_total_volume_usd'].max()
    print(top_pool_volumes)
    print(daily_protocol_stats)

    daily_protocol_stats['top_pool_concentration'] = (
        top_pool_volumes / daily_protocol_stats['total_volume_all_pools'] * 100
    )

    # Round numerical values
    daily_protocol_stats = daily_protocol_stats.round({
        'total_volume_all_pools': 2,
        'total_volume0_all_pools': 2,
        'total_volume1_all_pools': 2,
        'volume_ratio_all_pools': 3,
        'volume_7d_ma_all_pools': 2,
        'growth_rate_all_pools': 2,
        'avg_volume_per_pool': 2,
        'top_pool_concentration': 2
    })
    
    return daily_pool_stats, daily_protocol_stats

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
        df[col] = df[col].replace({
            'mUSDC': 'USDC',
            'mUSDT': 'USDT'
        })

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

@with_progress("Creating daily fees statistics")
def agg_fees_data(df):
    daily_fees = df.groupby(['timestamp']).agg(
            fees_0 =('totalFees0', 'sum'),
            fees_1 =('totalFees1', 'sum'),
            fees_0_usd = ('totalFees0_usd', 'sum'),
            fees_1_usd = ('totalFees1_usd', 'sum')
        ).reset_index().fillna(0)

    daily_fees['total_fees'] = daily_fees['fees_0_usd'] + daily_fees['fees_1_usd']

    return daily_fees

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

        # ==========================================================
        # Get raw subgraph data
        # ==========================================================

        # Get deposits data
        ProgressIndicators.print_step("Fetching pool deposits data", "start")
        deposits_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.POOLS_SUBGRAPH, 
            PoolQueries.GET_DEPOSITS, 
            'mints'
        )
        ProgressIndicators.print_step(f"Loaded {len(deposits_data)} deposit transactions", "success")

        # Get withdrawals data
        ProgressIndicators.print_step("Fetching pool withdrawals data", "start")
        withdrawals_data = SubgraphClient.get_subgraph_data(
            SubgraphClient.POOLS_SUBGRAPH, 
            PoolQueries.GET_WITHDRAWALS, 
            'burns'
        )
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
        
        # ==========================================================
        # Upload raw data to BigQuery
        # ==========================================================

        ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")
        
        fees_data['id'] = fees_data['id'].astype('int')
        volume_data['id'] = volume_data['id'].astype('int')

        raw_datasets = [
            (deposits_data, 'pool_deposits_raw', 'transactionHash_'),
            (withdrawals_data, 'pool_withdrawals_raw', 'transactionHash_'),
            (volume_data, 'pool_volume_raw', 'id'),
            (fees_data, 'pool_fees_raw', 'id')
        ]

        # for dataset, table_name, id_column in raw_datasets:
        #     if dataset is not None and len(dataset) > 0:
        #         bq.update_table(dataset, 'raw_data', table_name, id_column)
        #         ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # ==========================================================
        # Clean raw data
        # ==========================================================

        deposits_clean = process_pools_data(deposits_data, 'deposit')
        withdrawals_clean = process_pools_data(withdrawals_data, 'withdrawal')
        volume_clean = process_volume_data(volume_data)
        fees_clean = process_fees_data(fees_data)

        # ==========================================================
        # Upload clean data to BigQuery
        # ==========================================================

        ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
        
        volume_clean['id'] = volume_clean['id'].astype('int')
        fees_clean['id'] = fees_clean['id'].astype('int')

        clean_datasets = [
            (deposits_clean, 'pool_deposits_clean', 'transactionHash_'),
            (withdrawals_clean, 'pool_withdrawals_clean', 'transactionHash_'),
            (volume_clean, 'pool_volume_clean', 'id'),
            (fees_clean, 'pool_fees_clean', 'id')
        ]

        # for dataset, table_name, id_column in clean_datasets:
        #     if dataset is not None and len(dataset) > 0:
        #         bq.update_table(dataset, 'staging', table_name, id_column)
        #         ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # ==========================================================
        # Aggregate data
        # ==========================================================

        agg_volume = analyze_pool_volumes(volume_clean)
        agg_volume.to_csv('agg_volume.csv')

        agg_volume_by_pool, agg_all_volume = agg_volume_data(volume_clean)
        agg_volume_by_pool.to_csv('agg_volume_by_pool.csv')
        agg_all_volume.to_csv('agg_all_volume.csv')

        agg_tvl_by_pool, agg_all_tvl, tvl_snapshot = agg_pools_data(deposits_clean, withdrawals_clean)
        agg_tvl_by_pool.to_csv('agg_tvl_by_pool.csv')
        agg_all_tvl.to_csv('agg_all_tvl.csv')
        tvl_snapshot.to_csv('tvl_snapshot.csv')
        
        agg_fees = agg_fees_data(fees_clean)
        agg_fees.to_csv('agg_fees.csv')

        # save_datasets = [
        #     (agg_volume), (agg_volume_by_pool), (agg_all_volume), 
        #     (agg_tvl_by_pool), (agg_all_tvl), (agg_fees)
        # ]

        # for dataset in save_datasets:
        #     dataset.to_csv(f'{dataset}.csv')





        # daily_pools_by_pool = create_daily_pools_by_pool_df(deposits_clean, withdrawals_clean)
        # pool_summary = create_pools_summary_df(deposits_clean, withdrawals_clean)
        # daily_volume = aggregate_volume_by_pool_by_day(volume_clean)
        # daily_fees = aggregate_fees_by_day(fees_clean)
        
        # ==========================================================
        # Upload aggregate data to BigQuery
        # ==========================================================
        
        ProgressIndicators.print_step("Uploading aggregate data to BigQuery", "start")

        # agg_datasets = [
        #     (daily_pools_by_pool, 'agg_daily_pool_txns', 'timestamp_'),
        #     (daily_volume, 'agg_daily_volume', 'timestamp'),
        #     (daily_fees, 'agg_daily_fees', 'timestamp')
        # ]

        # for dataset, table_name, id_column in agg_datasets:
        #     if dataset is not None and len(dataset) > 0:
        #         bq.update_table(dataset, 'marts', table_name, id_column)
        #         ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # ProgressIndicators.print_step("Uploading pool breakdown data to BigQuery", "start")
        # if pool_summary is not None and len(pool_summary) > 0:
        #     bq.upsert_table_by_id(pool_summary, 'marts', 'agg_pools_by_pool', 'pool')
        #     ProgressIndicators.print_step("Uploaded pools breakdown to BigQuery", "success")

        ProgressIndicators.print_header("🚀 POOLS PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
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





# @with_progress("Creating daily pools by pool aggregation")
# def create_daily_pools_by_pool_df(deposits_df, withdrawals_df):
#     """Create daily pool activity data aggregated by pool"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     # Create aggregations by pool
#     daily_pools_by_pool = combined_df.groupby(['timestamp_', 'pool']).agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('sender', 'count')
#     ).assign(
#         total_deposit_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby(['timestamp_', 'pool'])['amount0_usd'].sum(),
#         total_deposit_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby(['timestamp_', 'pool'])['amount1_usd'].sum(),
#         total_withdrawal_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby(['timestamp_', 'pool'])['amount0_usd'].sum(),
#         total_withdrawal_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby(['timestamp_', 'pool'])['amount1_usd'].sum()
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

#     deposit_amt_0_cols = [col for col in daily_pools_by_pool_final.columns if col.startswith('total_deposit_amt_0')]
#     deposit_amt_1_cols = [col for col in daily_pools_by_pool_final.columns if col.startswith('total_deposit_amt_1')]
#     withdrawal_amt_0_cols = [col for col in daily_pools_by_pool_final.columns if col.startswith('total_deposit_amt_0')]
#     withdrawal_amt_1_cols = [col for col in daily_pools_by_pool_final.columns if col.startswith('total_deposit_amt_1')]
    
#     if deposit_amt_0_cols and deposit_amt_1_cols:
#         daily_pools_by_pool_final['total_deposit_amt'] = daily_pools_by_pool_final[deposit_amt_0_cols].sum(axis=1) + daily_pools_by_pool_final[deposit_amt_1_cols].sum(axis=1)

#     if withdrawal_amt_0_cols and withdrawal_amt_1_cols:
#         daily_pools_by_pool_final['total_withdrawal_amt'] = daily_pools_by_pool_final[withdrawal_amt_0_cols].sum(axis=1) + daily_pools_by_pool_final[withdrawal_amt_0_cols].sum(axis=1)
    
#     return daily_pools_by_pool_final

# @with_progress("Creating pool summary statistics")
# def create_pools_summary_df(deposits_df, withdrawals_df):
#     """Create summary statistics by pool"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     pool_summary = combined_df.groupby('pool').agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('sender', 'count')
#     ).assign(
#         total_deposit_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby('pool')['amount0_usd'].sum(),
#         total_deposit_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby('pool')['amount1_usd'].sum(),
#         total_withdrawal_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby('pool')['amount0_usd'].sum(),
#         total_withdrawal_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby('pool')['amount1_usd'].sum()
#     ).reset_index()

#     return pool_summary

# @with_progress("Creating daily pools aggregation")
# def create_daily_pools_df(deposits_df, withdrawals_df):
#     """Create daily aggregated pool activity data"""
#     # Combine deposits and withdrawals
#     combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
    
#     # Create aggregations
#     daily_pools = combined_df.groupby(['timestamp_']).agg(
#         total_deposits=('transaction_type', lambda x: (x == 'deposit').sum()),
#         total_withdrawals=('transaction_type', lambda x: (x == 'withdrawal').sum()),
#         total_transactions=('sender', 'count'),
#     ).assign(
#         total_deposit_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby('timestamp_')['amount0_usd'].sum(),
#         total_deposit_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'deposit'].groupby('timestamp_')['amount1_usd'].sum(),
#         total_withdrawal_amt_0 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby('timestamp_')['amount0_usd'].sum(),
#         total_withdrawal_amt_1 = lambda df: combined_df[combined_df['transaction_type'] == 'withdrawal'].groupby('timestamp_')['amount1_usd'].sum()
#     ).reset_index().fillna(0)

#     daily_pools['total_deposit_amt'] = daily_pools['total_deposit_amt_0'] + daily_pools['total_deposit_amt_1']
#     daily_pools['total_withdrawal_amt'] = daily_pools['total_withdrawal_amt_0'] + daily_pools['total_withdrawal_amt_1']

#     return daily_pools

# @with_progress("Creating daily volume statistics")
# def aggregate_volume_by_pool_by_day(df):
#     # Create daily volume aggregations by pool
#     daily_volume_by_pool = df.groupby(['timestamp', 'pool']).agg(
#         total_volume_0_usd = ('totalVolume0_usd', 'sum'),
#         total_volume_1_usd = ('totalVolume1_usd', 'sum')
#     ).reset_index()

#     # Pivot data by pool
#     vol_by_pools_pivot = daily_volume_by_pool.pivot(
#         index='timestamp', columns='pool'
#     ).fillna(0)

#     # Flatten column names
#     vol_by_pools_pivot.columns = [
#         '_'.join(col).strip() for col in vol_by_pools_pivot.columns.values
#     ]
#     daily_volume_by_pool_final = vol_by_pools_pivot.reset_index()

#     vol_0_cols = [col for col in daily_volume_by_pool_final.columns if col.startswith('total_volume_0')]
#     vol_1_cols = [col for col in daily_volume_by_pool_final.columns if col.startswith('total_volume_1')]

#     if vol_0_cols and vol_1_cols:
#         daily_volume_by_pool_final['total_volume'] = daily_volume_by_pool_final[vol_0_cols].sum(axis=1) + daily_volume_by_pool_final[vol_1_cols].sum(axis=1)

#     return daily_volume_by_pool_final
