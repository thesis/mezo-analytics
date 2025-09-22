from dotenv import load_dotenv
import pandas as pd
import numpy as np

from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from mezo.currency_utils import format_currency_columns, replace_token_labels
from mezo.data_utils import add_rolling_values
from mezo.currency_config import TOKEN_MAP, TOKENS_ID_MAP
from mezo.datetime_utils import format_datetimes
from mezo.currency_utils import get_token_prices
from mezo.clients import BigQueryClient
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

# ==================================================
# HELPER FUNCTIONS
# ==================================================

@with_progress("Converting tokens to USD")
def add_usd_conversions(df, token_column, tokens_id_map, amount_columns=None):
    """
    Add USD price conversions to any token data
    
    Args:
        df: DataFrame containing token data
        token_column: Name of column containing token identifiers
        tokens_id_map: Dictionary mapping tokens to CoinGecko IDs
        amount_columns: List of amount columns to convert, or None for auto-detection
    
    Returns:
        DataFrame with USD conversion columns added
    """
    if token_column not in df.columns:
        raise ValueError(f"Column '{token_column}' not found in DataFrame")
    
    # Get token prices
    prices = get_token_prices()
    if prices is None or prices.empty:
        raise ValueError("No token prices received from API")
    
    token_usd_prices = prices.T.reset_index()
    df_result = df.copy()
    df_result['index'] = df_result[token_column].map(tokens_id_map)
    
    df_with_usd = pd.merge(df_result, token_usd_prices, how='left', on='index')
    
    # Set MUSD price to 1.0 (1:1 with USD)
    df_with_usd.loc[df_with_usd[token_column] == 'MUSD', 'usd'] = 1.0
    
    # Auto-detect amount columns if not provided
    if amount_columns is None:
        amount_columns = [col for col in df.columns if 'amount' in col.lower() and col != 'amount_usd']
    
    # Add USD conversion for each amount column
    for col in amount_columns:
        if col in df_with_usd.columns:
            usd_col_name = f"{col}_usd" if not col.endswith('_usd') else col
            df_with_usd[usd_col_name] = df_with_usd[col] * df_with_usd['usd']
    
    return df_with_usd

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

# ==================================================
# RUN MAIN PROCESS
# ==================================================

def main():
    """Main function to process bridge transaction data."""
    ProgressIndicators.print_header("BRIDGE DATA PROCESSING PIPELINE")

    try:
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.5f}'.format
        
        # Load clients
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')

        ProgressIndicators.print_step("Environment loaded successfully", "success")
        
    # ==================================================
    # GET RAW BRIDGE DATA
    # ==================================================
        ProgressIndicators.print_step("Fetching raw bridge deposit data", "start")
        raw_deposits = SubgraphClient.get_subgraph_data(
            SubgraphClient.MEZO_BRIDGE_SUBGRAPH,
            BridgeQueries.GET_BRIDGE_TRANSACTIONS,
            'assetsLockeds'
        )
        ProgressIndicators.print_step(f"Retrieved {len(raw_deposits) if raw_deposits is not None else 0} deposit transactions", "success")

        ProgressIndicators.print_step("Fetching raw bridge withdrawal data", "start")
        raw_withdrawals = SubgraphClient.get_subgraph_data(
            SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH,
            BridgeQueries.GET_NATIVE_WITHDRAWALS,
            'assetsUnlockeds'
        )
        ProgressIndicators.print_step(f"Retrieved {len(raw_withdrawals) if raw_withdrawals is not None else 0} withdrawal transactions", "success")
        
        # Upload raw data to BigQuery
        ProgressIndicators.print_step("Uploading raw bridge data to BigQuery", "start")
        
        if raw_deposits is not None and len(raw_deposits) > 0:
            bq.update_table(raw_deposits, 'raw_data', 'bridge_transactions_raw', 'transactionHash_')
            ProgressIndicators.print_step("Uploaded raw bridge data to BigQuery", "success")

    # ==========================================================
    # UPLOAD RAW DATA TO BIGQUERY
    # ==========================================================

        ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
        raw_datasets = [
            (raw_deposits, 'bridge_transactions_raw', 'transactionHash_'),
            (raw_withdrawals, 'bridge_withdrawals_raw', 'transactionHash_'),
        ]

        for dataset, table_name, id_column in raw_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'raw_data', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

    # ==================================================
    # LOAD + CLEAN BRIDGE DATA
    # ==================================================
        ProgressIndicators.print_step("Processing deposits data", "start")
        deposits = clean_bridge_data(
            raw_deposits, 'timestamp_',
            ['timestamp_'], ['amount'], 'token'
        )

        deposits_with_usd = add_usd_conversions(
            deposits,
            token_column='token',
            tokens_id_map=TOKENS_ID_MAP,
            amount_columns =['amount']
        )

        deposits_with_usd['type'] = 'deposit'

        deposits_clean = deposits_with_usd.sort_values(
            by='timestamp_', ascending=True
        )

        deposits_clean = deposits_with_usd[[
            'timestamp_', 'amount', 'token', 'amount_usd',
            'recipient', 'transactionHash_', 'type'
        ]]

        deposits_clean = deposits_clean.rename(columns={'recipient': 'depositor'})
        
        ProgressIndicators.print_step(f"Processed {len(deposits_clean)} deposit records", "success")

        # clean withdrawals data
        ProgressIndicators.print_step("Processing withdrawals data", "start")
        withdrawals = clean_bridge_data(
            raw_withdrawals, 'timestamp_',
            ['timestamp_'], ['amount'], 'token'
        )

        withdrawals_with_usd = add_usd_conversions(
            withdrawals,
            token_column='token',
            tokens_id_map=TOKENS_ID_MAP,
            amount_columns=['amount']
        )

        withdrawals_with_usd['type'] = 'withdrawal'

        bridge_map = {'0': 'ethereum', '1': 'bitcoin'}
        withdrawals_with_usd['chain'] = withdrawals_with_usd['chain'].map(bridge_map)

        withdrawals_clean = withdrawals_with_usd[[
            'timestamp_', 'amount', 'token', 'amount_usd', 'chain',
            'recipient', 'sender', 'transactionHash_', 'type'
        ]]
        withdrawals_clean = withdrawals_clean.rename(columns={'sender': 'withdrawer', 'recipient': 'withdraw_recipient'})
        
        ProgressIndicators.print_step(f"Processed {len(withdrawals_clean)} withdrawal records", "success")

    # ==========================================================
    # UPLOAD CLEAN DATA TO BIGQUERY
    # ==========================================================
        ProgressIndicators.print_step("Uploading clean data to BigQuery", "start")
        
        clean_datasets = [
            (deposits_clean, 'bridge_deposits_clean', 'transactionHash_'),
            (withdrawals_clean, 'bridge_withdrawals_clean', 'transactionHash_'),
        ]

        for dataset, table_name, id_column in clean_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'staging', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

    # ==================================================
    # COMBINE DEPOSIT AND WITHDRAWAL DATA
    # ==================================================
        ProgressIndicators.print_step("Combining deposit and withdrawal data", "start")
        
        combined = pd.concat(
            [deposits_clean, withdrawals_clean], 
            ignore_index=True
        ).fillna(0)

        # Sort by timestamp for proper cumulative calculations
        combined = combined.sort_values('timestamp_').reset_index(drop=True)
        
        ProgressIndicators.print_step(f"Combined {len(combined)} total bridge transactions", "success")

    # ==================================================
    # CREATE BRIDGE VOLUME AND BRIDGE TVL DATAFRAMES
    # ==================================================
        ProgressIndicators.print_step("Calculating net flow, TVL, and volume", "start")
        
        # Calculate net amounts (deposits positive, withdrawals negative)
        combined['net_flow'] = np.where(
            combined['type'] == 'deposit',
            combined['amount_usd'],
            -combined['amount_usd']
        )
        # Calculate absolute amounts for deposits/withdrawals tracking
        combined['deposit_amount_usd'] = np.where(
            combined['type'] == 'deposit',
            combined['amount_usd'],
            0
        )

        combined['withdrawal_amount_usd'] = np.where(
            combined['type'] == 'withdrawal',
            combined['amount_usd'],
            0
        )

        combined['volume'] = combined['withdrawal_amount_usd'] + combined['deposit_amount_usd']
        combined['tvl'] = combined['net_flow'].cumsum()
        
        ProgressIndicators.print_step("Calculations complete", "success")

    # ==================================================
    # AGGREGATE TVL AND NET FLOW BY DAY
    # ==================================================
        ProgressIndicators.print_step("Aggregating TVL data by day", "start")

        tvl = combined.copy()

        daily_tvl = tvl.groupby(['timestamp_']).agg(
                # TVL metrics (end of day values)
                tvl = ('tvl', 'last'),
                net_flow = ('net_flow', 'sum'),  # Net daily flow
                deposits_usd = ('deposit_amount_usd', 'sum'),  # Total daily deposits
                withdrawals_usd = ('withdrawal_amount_usd', 'sum'),  # Total daily withdrawals
                tx_type = ('type', 'count'),  # Total transactions
            ).reset_index()

        # Calculate deposit and withdrawal counts
        deposit_counts = tvl[tvl['type'] == 'deposit'].groupby(['timestamp_']).size()
        depositors = tvl[tvl['type'] == 'deposit'].groupby(['timestamp_'])['depositor'].nunique()
        withdrawal_counts = tvl[tvl['type'] == 'withdrawal'].groupby(['timestamp_']).size()
        withdrawers = tvl[tvl['type'] == 'withdrawal'].groupby(['timestamp_'])['withdrawer'].nunique()
            
        # Add transaction counts to daily metrics
        daily_tvl = daily_tvl.set_index(['timestamp_'])
        daily_tvl['deposits'] = deposit_counts
        daily_tvl['depositors'] = depositors
        daily_tvl['withdrawals'] = withdrawal_counts
        daily_tvl['withdrawers'] = withdrawers
        daily_tvl['unique_wallets'] = daily_tvl['withdrawers'] + daily_tvl['depositors']
        daily_tvl['total_transactions'] = daily_tvl['deposits'] + daily_tvl['withdrawals']
        daily_tvl = daily_tvl.fillna(0).reset_index()

        # Calculate protocol-wide additional metrics
        daily_tvl['deposit_withdrawal_ratio'] = np.where(
            daily_tvl['withdrawals'] > 0,
            daily_tvl['deposits'] / daily_tvl['withdrawals'],
            np.inf
        )
            
        # TVL - no moving average, but track changes
        daily_tvl['tvl_change'] = daily_tvl['tvl'].diff()
        daily_tvl['tvl_change_pct'] = daily_tvl['tvl'].pct_change()
        daily_tvl['tvl_ath'] = daily_tvl['tvl'].cummax()

        daily_tvl['drawdown_from_ath'] = (
            (daily_tvl['tvl'] - daily_tvl['tvl_ath']) / 
            daily_tvl['tvl_ath']
        )
        for col in ['deposits', 'withdrawals', 'net_flow']:
            daily_tvl[f'{col}_ma7'] = daily_tvl[col].rolling(window=7).mean()
            daily_tvl[f'{col}_ma30'] = daily_tvl[col].rolling(window=7).mean()

        daily_tvl = daily_tvl.fillna(0)

        ProgressIndicators.print_step("Aggregation complete", "success")

    # ========================================
    # ADD VOLUME TO DAILY AGGREGATE DF
    # ========================================
        ProgressIndicators.print_step("Aggregating volume data by day", "start")

        daily_volume = daily_tvl.copy()

        daily_volume['volume'] = daily_volume['withdrawals_usd'] + daily_volume['deposits_usd']
        daily_volume['volume_7d_ma'] = daily_volume['volume'].rolling(window=7).mean()
        daily_volume['volume_30d_ma'] = daily_volume['volume'].rolling(window=30).mean()
        daily_volume['volume_change'] = daily_volume['volume'].pct_change()
        daily_volume['volume_change_7d'] = daily_volume['volume_7d_ma'].pct_change()
        daily_volume['volume_change_30d'] = daily_volume['volume_30d_ma'].pct_change()
        daily_volume['is_significant_volume'] = daily_volume['volume'].transform(lambda x: x > x.quantile(0.9))
        daily_volume = daily_volume.fillna(0)

        ProgressIndicators.print_step("Aggregation complete", "success")

    # ========================================
    # AGGREGATE BRIDGE VOLUME STATS (PER TOKEN)
    # ========================================

        ProgressIndicators.print_step("Aggregating bridge volume stats by token", "start")
        # Make a copy to avoid modifying original
        daily_brige_vol_by_token = bridge_volume.copy()

        # Ensure timestamp is datetime and create date column
        daily_brige_vol_by_token['timestamp_'] = pd.to_datetime(daily_brige_vol_by_token['timestamp_'])
        daily_brige_vol_by_token['timestamp_'] = daily_brige_vol_by_token['timestamp_'].dt.date

        # Sort by pool and timestamp for proper calculations
        daily_brige_vol_by_token = daily_brige_vol_by_token.sort_values(['token', 'timestamp_'])

        # Calculate daily volume (difference from previous day for each pool)
        daily_brige_vol_by_token['daily_token_volume'] = daily_brige_vol_by_token.groupby('token')['volume'].diff().fillna(0)

        # For first entry of each pool, use total as daily
        first_entries = daily_brige_vol_by_token.groupby('token').first().index
        mask = daily_brige_vol_by_token.set_index('token').index.isin(first_entries)
        daily_brige_vol_by_token.loc[mask & daily_brige_vol_by_token['volume'].isna(), 'volume'] = \
            daily_brige_vol_by_token.loc[mask & daily_brige_vol_by_token['volume'].isna(), 'volume']

        daily_brige_vol_by_token = daily_brige_vol_by_token.groupby(['timestamp_', 'token']).agg({
                'daily_token_volume': 'last',  # Daily volume for the token
            }).reset_index()

        # Calculate 7-day moving average for each pool
        daily_brige_vol_by_token['volume_7d_ma'] = daily_brige_vol_by_token.groupby('token')[
            'daily_token_volume'].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
        )

        # Calculate growth rate (day-over-day percentage change)
        daily_brige_vol_by_token['growth_rate'] = daily_brige_vol_by_token.groupby('token')[
            'daily_token_volume'].transform(
                lambda x: x.pct_change() * 100
        )

        # Identify significant volume days (> 90th percentile for each pool)
        daily_brige_vol_by_token['is_significant_volume'] = daily_brige_vol_by_token.groupby('token')[
            'daily_token_volume'].transform(
                lambda x: x > x.quantile(0.9)
        )

        # Add token rank by daily volume
        daily_brige_vol_by_token['daily_rank'] = daily_brige_vol_by_token.groupby('timestamp_')[
            'daily_token_volume'].rank(
                method='dense', ascending=False
        )
        ProgressIndicators.print_step(f"Generated daily volume stats for {daily_brige_vol_by_token['token'].nunique()} tokens", "success")

    # ========================================
    # AGGREGATE DAILY VOLUME STATS (ALL TOKENS)
    # ========================================
        ProgressIndicators.print_step("Aggregating protocol-wide volume stats", "start")
        daily_bridge_volume = daily_brige_vol_by_token.groupby('timestamp_').agg({
            'daily_token_volume': 'sum',      # Total volume across all pools
            'is_significant_volume': 'sum',       # Count of pools with significant volume
            'token': 'count'                  # Number of active pools
        }).reset_index()

        # Rename columns for clarity
        daily_bridge_volume.columns = [
            'timestamp_',
            'total_volume',
            'tokens_with_significant_volume',
            'tokens_count'
        ]

        # Calculate 7-day moving average for protocol
        daily_bridge_volume['volume_7d_ma'] = daily_bridge_volume['total_volume'].rolling(
            window=7, min_periods=1
        ).mean()

        # Calculate protocol-wide growth rate
        daily_bridge_volume['growth_rate'] = daily_bridge_volume['total_volume'].pct_change()

        # Identify significant volume days for protocol (> 90th percentile)
        threshold = daily_bridge_volume['total_volume'].quantile(0.9)
        daily_bridge_volume['is_significant_volume_day'] = daily_bridge_volume['total_volume'] > threshold

        # Add some additional protocol metrics
        daily_bridge_volume['avg_volume_per_token'] = (
            daily_bridge_volume['total_volume'] / daily_bridge_volume['tokens_count']
        )
        ProgressIndicators.print_step(f"Protocol volume aggregation complete ({len(daily_bridge_volume)} days)", "success")

    # ========================================
    # AGGREGATE NET FLOW AND TVL (PER TOKEN)
    # ========================================
        ProgressIndicators.print_step("Calculating TVL and flow metrics by token", "start")
        tvl_df = combined.copy()

        daily_tvl_by_token = tvl_df.groupby(['timestamp_', 'token']).agg({
                # TVL metrics (end of day values)
                'tvl': 'last',
                'net_flow': 'sum',  # Net daily flow
                'deposit_amount_usd': 'sum',  # Total daily deposits
                'withdrawal_amount_usd': 'sum',  # Total daily withdrawals
                'type': 'count',  # Total transactions
                'sender': 'nunique'  # Unique addresses
            }).reset_index()
            
        # Calculate deposit and withdrawal counts
        deposit_counts = combined[combined['type'] == 'deposit'].groupby(['timestamp_', 'token']).size()
        withdrawal_counts = combined[combined['type'] == 'withdrawal'].groupby(['timestamp_', 'token']).size()
            
        # Add transaction counts to daily metrics
        daily_tvl_by_token = daily_tvl_by_token.set_index(['timestamp_', 'token'])
        daily_tvl_by_token['deposit_count'] = deposit_counts
        daily_tvl_by_token['withdrawal_count'] = withdrawal_counts
        daily_tvl_by_token = daily_tvl_by_token.fillna(0).reset_index()
            
        # Rename columns for clarity
        daily_tvl_by_token.columns = [
            'timestamp_', 'token', 'tvl', 'net_flow', 'deposits_usd', 'withdrawals_usd',
            'total_transactions', 'unique_users', 'deposits', 'withdrawals'
        ]

        # Calculate additional metrics
        daily_tvl_by_token['deposit_withdrawal_ratio'] = np.where(
            daily_tvl_by_token['withdrawals_usd'] > 0,
            daily_tvl_by_token['deposits_usd'] / daily_tvl_by_token['withdrawals_usd'],
            np.inf
        )
            
        daily_tvl_by_token['tvl_change'] = daily_tvl_by_token.groupby('token')['tvl'].diff()
        daily_tvl_by_token['tvl_change_pct'] = daily_tvl_by_token.groupby('token')['tvl'].pct_change()
            
        # Calculate 7-day moving averages for key metrics
        add_rolling_values(daily_tvl_by_token, 7, ['tvl', 'deposits_usd', 'withdrawals_usd', 'net_flow'])
        ProgressIndicators.print_step(f"TVL metrics calculated for {daily_tvl_by_token['token'].nunique()} tokens", "success")
            
    # =========================================
    # GET TOTAL DAILY TVL AND FLOW (ALL TOKENS)
    # =========================================
        ProgressIndicators.print_step("Calculating protocol-wide TVL and flow metrics", "start")
        daily_tvl = daily_tvl_by_token.groupby('timestamp_').agg({
                # TVL metrics (sum across all pools)
                'tvl': 'sum',
                'net_flow': 'sum',
                'deposits_usd': 'sum',
                'withdrawals_usd': 'sum',
                'total_transactions': 'sum',
                'deposits': 'sum',
                'withdrawals': 'sum',
                'unique_users': 'sum',  # Note: might count same user across pools
                'token': 'count'  # Number of active pools
            }).reset_index()
            
        # Calculate protocol-wide additional metrics
        daily_tvl['protocol_deposit_withdrawal_ratio'] = np.where(
            daily_tvl['withdrawals'] > 0,
            daily_tvl['deposits'] / daily_tvl['withdrawals'],
            np.inf
        )
            
        daily_tvl['tvl_change'] = daily_tvl['tvl'].diff()
        daily_tvl['tvl_change_pct'] = daily_tvl['tvl'].pct_change()
            
        # Calculate 7-day moving averages for protocol metrics
        add_rolling_values(daily_tvl, 7, ['tvl', 'deposits_usd', 'withdrawals_usd', 'net_flow'])

        # Identify high activity days
        daily_tvl['high_deposit_day'] = daily_tvl['deposits'] > daily_tvl['deposits'].quantile(0.9)
        daily_tvl['high_withdrawal_day'] = daily_tvl['withdrawals'] > daily_tvl['withdrawals'].quantile(0.9)

        # Calculate average metrics per pool
        daily_tvl['avg_tvl_per_token'] = daily_tvl['tvl'] / daily_tvl['token']
        daily_tvl['avg_deposits_per_token'] = daily_tvl['deposits'] / daily_tvl['token']
        daily_tvl['avg_withdrawals_per_token'] = daily_tvl['withdrawals'] / daily_tvl['token']

        ProgressIndicators.print_step(f"Protocol metrics aggregation complete ({len(daily_tvl)} days)", "success")
    # ==========================================================
    # UPLOAD FINAL DATA TO BIGQUERY
    # ==========================================================

        ProgressIndicators.print_step("Uploading final data to BigQuery", "start")
        marts_datasets = [
            (daily_brige_vol_by_token, 'marts_daily_bridge_vol_by_token', 'timestamp_'),
            (daily_bridge_volume, 'marts_daily_bridge_volume', 'timestamp_'),
            (daily_tvl_by_token, 'marts_daily_bridge_tvl_by_token', 'timestamp_'),
            (daily_tvl, 'marts_daily_tvl', 'timestamp_')
        ]

        for dataset, table_name, id_column in marts_datasets:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'marts', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")


        ProgressIndicators.print_header(f"{ProgressIndicators.SUCCESS} BRIDGE DATA PROCESSING COMPLETE")
    
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