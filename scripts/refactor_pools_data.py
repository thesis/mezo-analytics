"""
Refactored Pool Data Processing Pipeline
Processes liquidity pool data for Mezo protocol
"""

from dotenv import load_dotenv
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

from mezo.currency_utils import (
    format_pool_token_columns, 
    add_pool_usd_conversions, 
    format_token_columns, 
    add_usd_conversions
)
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import flatten_json_column
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.currency_config import (
    POOLS_MAP, 
    POOL_TOKEN_PAIRS, 
    TOKENS_ID_MAP, 
    TIGRIS_MAP
)
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress
from mezo.queries import PoolQueries


@dataclass
class PoolMetrics:
    """Container for all pool metrics"""
    tvl_metrics: pd.DataFrame
    volume_metrics: pd.DataFrame
    fee_metrics: pd.DataFrame
    user_metrics: pd.DataFrame
    health_metrics: pd.DataFrame
    summary_stats: Dict


class PoolDataProcessor:
    """Main class for processing pool data"""
    
    def __init__(self, bq_client: BigQueryClient):
        self.bq = bq_client
        self.metrics = {}
        
    @with_progress("Fetching all pool data")
    def fetch_pool_data(self) -> Dict[str, pd.DataFrame]:
        """Fetch all pool data from subgraphs"""
        
        data_sources = {
            'deposits': (SubgraphClient.POOLS_SUBGRAPH, PoolQueries.GET_DEPOSITS, 'mints'),
            'withdrawals': (SubgraphClient.POOLS_SUBGRAPH, PoolQueries.GET_WITHDRAWALS, 'burns'),
            'volume': (SubgraphClient.TIGRIS_POOLS_SUBGRAPH, PoolQueries.GET_POOL_VOLUME, 'poolVolumes'),
            'fees': (SubgraphClient.TIGRIS_POOLS_SUBGRAPH, PoolQueries.GET_TOTAL_POOL_FEES, 'feesStats_collection')
        }
        
        raw_data = {}
        for name, (subgraph, query, key) in data_sources.items():
            ProgressIndicators.print_step(f"Fetching {name} data", "start")
            data = SubgraphClient.get_subgraph_data(subgraph, query, key)
            raw_data[name] = data
            ProgressIndicators.print_step(f"Loaded {len(data)} {name} records", "success")
            
        return raw_data
    
    @with_progress("Processing pool transactions")
    def process_transactions(self, deposits_df: pd.DataFrame, withdrawals_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process deposit and withdrawal transactions"""
        
        # Process deposits
        deposits = self._clean_transaction_data(deposits_df, 'deposit')
        
        # Process withdrawals  
        withdrawals = self._clean_transaction_data(withdrawals_df, 'withdrawal')
        
        # Combine for analysis
        all_transactions = pd.concat([deposits, withdrawals], ignore_index=True)
        all_transactions = all_transactions.sort_values('timestamp_').reset_index(drop=True)
        
        return deposits, withdrawals, all_transactions
    
    def _clean_transaction_data(self, df: pd.DataFrame, tx_type: str) -> pd.DataFrame:
        """Clean and format transaction data"""
        
        clean_df = df.copy()
        clean_df['pool'] = clean_df['contractId_'].map(POOLS_MAP)
        clean_df = format_datetimes(clean_df, ['timestamp_'])
        clean_df = format_pool_token_columns(clean_df, 'contractId_', POOL_TOKEN_PAIRS)
        clean_df = add_pool_usd_conversions(clean_df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
        clean_df['transaction_type'] = tx_type
        
        # Add transaction value
        clean_df['transaction_value_usd'] = clean_df['amount0_usd'] + clean_df['amount1_usd']
        
        return clean_df
    
    @with_progress("Calculating TVL metrics")
    def calculate_tvl_metrics(self, deposits: pd.DataFrame, withdrawals: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive TVL metrics"""
        
        # Combine transactions
        all_txns = pd.concat([deposits, withdrawals], ignore_index=True)
        all_txns = all_txns.sort_values(['pool', 'timestamp_']).reset_index(drop=True)
        
        # Calculate net amounts
        all_txns['net_value_usd'] = np.where(
            all_txns['transaction_type'] == 'deposit',
            all_txns['transaction_value_usd'],
            -all_txns['transaction_value_usd']
        )
        
        # Calculate cumulative TVL by pool
        all_txns['tvl_usd'] = all_txns.groupby('pool')['net_value_usd'].cumsum()
        
        # Daily TVL snapshots
        daily_tvl = all_txns.groupby(['timestamp_', 'pool']).agg({
            'tvl_usd': 'last',
            'net_value_usd': 'sum',
            'transaction_type': 'count'
        }).reset_index()
        
        daily_tvl.columns = ['date', 'pool', 'tvl', 'daily_net_flow', 'transaction_count']
        
        # Add growth metrics
        daily_tvl['tvl_growth'] = daily_tvl.groupby('pool')['tvl'].pct_change()
        daily_tvl['tvl_7d_ma'] = daily_tvl.groupby('pool')['tvl'].transform(
            lambda x: x.rolling(7, min_periods=1).mean()
        )
        
        # Protocol-wide TVL
        protocol_tvl = daily_tvl.groupby('date').agg({
            'tvl': 'sum',
            'daily_net_flow': 'sum',
            'transaction_count': 'sum'
        }).reset_index()
        protocol_tvl['pool'] = 'ALL_POOLS'
        
        # Combine pool and protocol metrics
        tvl_metrics = pd.concat([daily_tvl, protocol_tvl], ignore_index=True)
        
        return tvl_metrics
    
    @with_progress("Calculating volume metrics")
    def calculate_volume_metrics(self, volume_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive volume metrics"""
        
        # Check if volume data exists
        if volume_df is None or len(volume_df) == 0:
            ProgressIndicators.print_step("No volume data available, creating empty volume metrics", "warning")
            return pd.DataFrame(columns=['timestamp', 'pool', 'volume', 'volume_token0', 'volume_token1', 'volume_7d_ma', 'volume_growth', 'token_volume_ratio'])
        
        # Clean volume data
        df = volume_df.copy()
        
        # First, check if 'pool' column exists and contains dictionaries
        if 'pool' in df.columns:
            # Check if pool column contains dictionaries
            first_pool = df['pool'].iloc[0] if len(df) > 0 else None
            if isinstance(first_pool, dict):
                # Pool column contains actual dict objects - flatten it
                df = flatten_json_column(df, 'pool')
        
        # If no 'pool' column yet, look for columns to flatten
        if 'pool' not in df.columns or df['pool'].dtype == 'object':
            # Look for nested pool data
            for col in df.columns:
                if col == 'pool' and df[col].dtype == 'object':
                    # Check if it's a dict column
                    sample = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(sample, dict):
                        df = flatten_json_column(df, col)
                        break
        
        # Format timestamps
        if 'timestamp' in df.columns:
            df = format_datetimes(df, ['timestamp'])
            df.rename(columns={'timestamp': 'date'}, inplace=True)
        elif 'timestamp_' in df.columns:
            df = format_datetimes(df, ['timestamp_'])
            df.rename(columns={'timestamp_': 'date'}, inplace=True)
        
        # Map pool names - now pool_name should exist after flattening
        if 'pool_name' in df.columns:
            df['pool'] = df['pool_name'].map(TIGRIS_MAP)
            # Use pool_name as fallback if mapping returns NaN
            df['pool'] = df['pool'].fillna(df['pool_name'])
        elif 'pool' not in df.columns or df['pool'].isna().all():
            df['pool'] = 'unknown'
        
        # Ensure pool column is string type, not dict
        df['pool'] = df['pool'].astype(str)
        
        # Format token amounts
        df = self._format_volume_amounts(df)
        
        # Sort by pool and date BEFORE calculating volumes
        df = df.sort_values(['pool', 'date'])
        
        # Check if volumes are already daily or cumulative
        # If totalVolume fields exist, they're likely cumulative
        if 'totalVolume0_usd' in df.columns or 'totalVolume1_usd' in df.columns:
            # These are cumulative volumes, calculate daily differences
            
            # Calculate daily volume for token0
            if 'totalVolume0_usd' in df.columns:
                df['daily_volume0_usd'] = df.groupby('pool')['totalVolume0_usd'].diff()
                # First day of each pool gets the total as daily volume
                first_day_mask = df.groupby('pool').head(1).index
                df.loc[first_day_mask, 'daily_volume0_usd'] = df.loc[first_day_mask, 'totalVolume0_usd']
            else:
                df['daily_volume0_usd'] = 0
                
            # Calculate daily volume for token1
            if 'totalVolume1_usd' in df.columns:
                df['daily_volume1_usd'] = df.groupby('pool')['totalVolume1_usd'].diff()
                first_day_mask = df.groupby('pool').head(1).index
                df.loc[first_day_mask, 'daily_volume1_usd'] = df.loc[first_day_mask, 'totalVolume1_usd']
            else:
                df['daily_volume1_usd'] = 0
            
            # Total daily volume
            df['daily_volume_usd'] = df['daily_volume0_usd'].fillna(0) + df['daily_volume1_usd'].fillna(0)
            df['daily_volume_usd'] = np.maximum(
                df['daily_volume0_usd'].fillna(0).sum(), 
                df['daily_volume1_usd'].fillna(0).sum()
            )
            
        else:
            # No cumulative volume data found
            df['daily_volume_usd'] = 0
            df['daily_volume0_usd'] = 0
            df['daily_volume1_usd'] = 0
        
        # Debug: Print sample of calculated daily volumes
        print(f"Sample daily volumes calculated:")
        print(df[['date', 'pool', 'daily_volume_usd']].head(10))
        
        # Volume metrics aggregation - use the daily volumes we calculated
        volume_metrics = df.groupby(['date', 'pool']).agg({
            'daily_volume_usd': 'sum',  # Sum in case there are multiple records per day
            'daily_volume0_usd': 'sum',
            'daily_volume1_usd': 'sum'
        }).reset_index()
        
        # Rename columns
        volume_metrics.columns = ['date', 'pool', 'volume', 'volume_token0', 'volume_token1']
        
        # Add analytics
        volume_metrics['volume_7d_ma'] = volume_metrics.groupby('pool')['volume'].transform(
            lambda x: x.rolling(7, min_periods=1).mean()
        )
        volume_metrics['volume_growth'] = volume_metrics.groupby('pool')['volume'].pct_change()
        
        # Token ratio (shows which token is traded more)
        volume_metrics['token_volume_ratio'] = volume_metrics['volume_token0'] / (
            volume_metrics['volume_token1'] + 1e-10
        )
        
        # Clean up NaN and inf values
        volume_metrics = volume_metrics.replace([np.inf, -np.inf], 0)
        volume_metrics = volume_metrics.fillna(0)
        
        # Debug: Print final volume metrics
        print(f"Final volume metrics summary:")
        print(f"Total daily volume across all pools: ${volume_metrics['volume'].sum():,.2f}")
        print(f"Latest day volume: ${volume_metrics.groupby('date')['volume'].sum().iloc[-1] if len(volume_metrics) > 0 else 0:,.2f}")
        
        return volume_metrics
    
    def _format_volume_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format volume token amounts"""
        
        # Replace mUSDC/mUSDT with standard names
        token_cols = ['pool_token0_symbol', 'pool_token1_symbol']
        for col in token_cols:
            if col in df.columns:
                df[col] = df[col].replace({'mUSDC': 'USDC', 'mUSDT': 'USDT'})
        
        # Format amounts
        if 'pool_token0_symbol' in df.columns:
            df = format_token_columns(df, ['totalVolume0'], 'pool_token0_symbol')
            df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalVolume0'])
            
        if 'pool_token1_symbol' in df.columns:
            df = format_token_columns(df, ['totalVolume1'], 'pool_token1_symbol')
            df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalVolume1'])
            
        return df
    
    @with_progress("Calculating fee metrics")
    def calculate_fee_metrics(self, fees_df: pd.DataFrame, volume_metrics: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive fee metrics"""
        
        # Check if fees data exists
        if fees_df is None or len(fees_df) == 0:
            ProgressIndicators.print_step("No fee data available, creating empty fee metrics", "warning")
            # Return empty dataframe with expected columns
            return pd.DataFrame(columns=['date', 'pool', 'daily_fees', 'fee_rate', 'cumulative_fees'])
        
        # Clean fee data
        df = fees_df.copy()
        
        # First, check if 'pool' column exists and contains dictionaries
        if 'pool' in df.columns:
            first_pool = df['pool'].iloc[0] if len(df) > 0 else None
            if isinstance(first_pool, dict):
                # Pool column contains actual dict objects - flatten it
                df = flatten_json_column(df, 'pool')
        
        # If no proper pool column yet, look for nested structure
        if 'pool' not in df.columns or df['pool'].dtype == 'object':
            for col in df.columns:
                if col == 'pool' and df[col].dtype == 'object':
                    sample = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(sample, dict):
                        df = flatten_json_column(df, col)
                        break
        
        # Format timestamps
        if 'timestamp' in df.columns:
            df = format_datetimes(df, ['timestamp'])
            df.rename(columns={'timestamp': 'date'}, inplace=True)
        elif 'timestamp_' in df.columns:
            df = format_datetimes(df, ['timestamp_'])
            df.rename(columns={'timestamp_': 'date'}, inplace=True)
        
        # Map pool names if available
        if 'pool_name' in df.columns:
            df['pool'] = df['pool_name'].map(TIGRIS_MAP)
            # Use pool_name as fallback if mapping returns NaN
            df['pool'] = df['pool'].fillna(df['pool_name'])
        elif 'pool' not in df.columns or df['pool'].isna().all():
            df['pool'] = 'unknown'
        
        # Ensure pool column is string type, not dict
        df['pool'] = df['pool'].astype(str)
        
        # Format fee amounts
        df = self._format_fee_amounts(df)
        
        # Sort by pool and date for proper difference calculation
        df = df.sort_values(['pool', 'date'])
        
        # Check if fees are cumulative (totalFees) or daily
        if 'totalFees0_usd' in df.columns or 'totalFees1_usd' in df.columns:
            # These are cumulative fees, calculate daily differences
            
            # Calculate daily fees for token0
            if 'totalFees0_usd' in df.columns:
                df['daily_fees0_usd'] = df.groupby('pool')['totalFees0_usd'].diff()
                # First day gets the total as daily fee
                first_day_mask = df.groupby('pool').head(1).index
                df.loc[first_day_mask, 'daily_fees0_usd'] = df.loc[first_day_mask, 'totalFees0_usd']
            else:
                df['daily_fees0_usd'] = 0
            
            # Calculate daily fees for token1
            if 'totalFees1_usd' in df.columns:
                df['daily_fees1_usd'] = df.groupby('pool')['totalFees1_usd'].diff()
                first_day_mask = df.groupby('pool').head(1).index
                df.loc[first_day_mask, 'daily_fees1_usd'] = df.loc[first_day_mask, 'totalFees1_usd']
            else:
                df['daily_fees1_usd'] = 0
                
            # Total daily fees
            df['total_fees_usd'] = df['daily_fees0_usd'].fillna(0) + df['daily_fees1_usd'].fillna(0)
        else:
            # No fee data found
            df['total_fees_usd'] = 0
        
        # Debug: Print sample of calculated daily fees
        print(f"Sample daily fees calculated:")
        print(df[['date', 'pool', 'total_fees_usd']].head(10))
        
        # Daily fee metrics - aggregate in case there are multiple records per day
        fee_metrics = df.groupby(['date', 'pool']).agg({
            'total_fees_usd': 'sum'
        }).reset_index()
        
        fee_metrics.columns = ['date', 'pool', 'daily_fees']
        
        # Merge with volume to calculate fee/volume ratio
        if volume_metrics is not None and len(volume_metrics) > 0:
            # Ensure pool column types match for merge
            volume_metrics['pool'] = volume_metrics['pool'].astype(str)
            fee_metrics = fee_metrics.merge(
                volume_metrics[['date', 'pool', 'volume']], 
                on=['date', 'pool'], 
                how='left'
            )
            
            # Calculate fee rate (fee as percentage of volume)
            fee_metrics['fee_rate'] = np.where(
                fee_metrics['volume'] > 0,
                fee_metrics['daily_fees'] / fee_metrics['volume'],
                0
            )
        else:
            fee_metrics['volume'] = 0
            fee_metrics['fee_rate'] = 0
        
        # Calculate cumulative fees
        fee_metrics = fee_metrics.sort_values(['pool', 'date'])
        fee_metrics['cumulative_fees'] = fee_metrics.groupby('pool')['daily_fees'].cumsum()
        
        # Clean up NaN values and cap fee_rate at reasonable values
        fee_metrics = fee_metrics.fillna(0)
        fee_metrics['fee_rate'] = fee_metrics['fee_rate'].clip(upper=0.01)  # Cap at 1% fee rate
        
        # Debug: Print final fee metrics
        print(f"Final fee metrics summary:")
        print(f"Total daily fees across all pools: ${fee_metrics['daily_fees'].sum():,.2f}")
        print(f"Latest day fees: ${fee_metrics.groupby('date')['daily_fees'].sum().iloc[-1] if len(fee_metrics) > 0 else 0:,.2f}")
        
        return fee_metrics
    
    def _format_fee_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format fee token amounts"""
        
        # Similar to volume formatting
        token_cols = ['pool_token0_symbol', 'pool_token1_symbol']
        for col in token_cols:
            if col in df.columns:
                df[col] = df[col].replace({'mUSDC': 'USDC', 'mUSDT': 'USDT'})
        
        if 'totalFees0' in df.columns and 'pool_token0_symbol' in df.columns:
            df = format_token_columns(df, ['totalFees0'], 'pool_token0_symbol')
            df = add_usd_conversions(df, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalFees0'])
            
        if 'totalFees1' in df.columns and 'pool_token1_symbol' in df.columns:
            df = format_token_columns(df, ['totalFees1'], 'pool_token1_symbol')
            df = add_usd_conversions(df, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalFees1'])
            
        return df
    
    @with_progress("Calculating user metrics")
    def calculate_user_metrics(self, deposits: pd.DataFrame, withdrawals: pd.DataFrame) -> pd.DataFrame:
        """Calculate user engagement metrics"""
        
        # Combine transactions
        all_txns = pd.concat([deposits, withdrawals], ignore_index=True)
        
        # Daily user metrics
        user_metrics = all_txns.groupby(['timestamp_', 'pool']).agg({
            'sender': 'nunique',
            'transaction_value_usd': ['mean', 'median', 'sum'],
            'transaction_type': 'count'
        }).reset_index()
        
        user_metrics.columns = [
            'date', 'pool', 'unique_users', 
            'avg_tx_size', 'median_tx_size', 'total_volume', 'tx_count'
        ]
        
        # User retention (users active in consecutive periods)
        user_metrics['returning_users'] = user_metrics.groupby('pool')['unique_users'].transform(
            lambda x: x.rolling(2, min_periods=2).apply(lambda y: min(y))
        )
        
        user_metrics['user_retention_rate'] = (
            user_metrics['returning_users'] / user_metrics['unique_users'].shift(1)
        )
        
        # New users (approximation - users not seen in last 7 days)
        user_metrics['new_users'] = user_metrics['unique_users'] - user_metrics.get('returning_users', 0)
        
        return user_metrics
    
    @with_progress("Calculating pool health metrics")
    def calculate_health_metrics(self, tvl_metrics: pd.DataFrame, volume_metrics: pd.DataFrame, 
                                fee_metrics: pd.DataFrame) -> pd.DataFrame:
        """Calculate pool health and efficiency metrics"""
        
        # Merge key metrics with outer join to preserve all data
        health = tvl_metrics.merge(volume_metrics, on=['date', 'pool'], how='outer')
        health = health.merge(fee_metrics[['date', 'pool', 'daily_fees', 'fee_rate']], 
                             on=['date', 'pool'], how='left')
        
        # Fill NaN values with appropriate defaults
        health['tvl'] = health['tvl'].fillna(0)
        health['volume'] = health['volume'].fillna(0)
        health['daily_fees'] = health['daily_fees'].fillna(0)
        health['fee_rate'] = health['fee_rate'].fillna(0)
        health['daily_net_flow'] = health['daily_net_flow'].fillna(0)
        
        # Calculate health indicators with NaN handling
        health['volume_tvl_ratio'] = np.where(
            health['tvl'] > 0,
            health['volume'] / health['tvl'],
            0
        )
        
        # Calculate fee APY more carefully
        # APY = (daily_fees * 365 / tvl) * 100
        # Cap at reasonable values (e.g., 1000% APY max)
        health['fee_apy'] = np.where(
            health['tvl'] > 1000,  # Only calculate APY if TVL > $1000
            (health['daily_fees'] * 365) / health['tvl'],
            0
        )
        # Cap APY at 10 (1000%)
        health['fee_apy'] = health['fee_apy'].clip(upper=10)
        
        # Deposit/Withdrawal ratio (from daily_net_flow)
        health['flow_stability'] = health.groupby('pool')['daily_net_flow'].transform(
            lambda x: x.rolling(7, min_periods=1).std()
        ).fillna(0)
        
        # Normalize flow stability to 0-1 range
        max_stability = health['flow_stability'].max() if health['flow_stability'].max() > 0 else 1
        health['flow_stability_normalized'] = 1 - (health['flow_stability'] / (max_stability + 1))
        
        # Pool efficiency score (composite metric) with normalized components
        # Normalize each component to 0-1 range before combining
        
        # Volume/TVL ratio: cap at 10 for normalization
        volume_tvl_normalized = (health['volume_tvl_ratio'].clip(upper=10) / 10)
        
        # Fee APY: already capped at 10, normalize to 0-1
        fee_apy_normalized = health['fee_apy'] / 10
        
        # Calculate efficiency score (0-100 scale)
        health['efficiency_score'] = (
            volume_tvl_normalized * 40 +           # Capital efficiency (40%)
            fee_apy_normalized * 30 +              # Revenue generation (30%)
            health['flow_stability_normalized'] * 30  # Stability (30%)
        )
        
        # Concentration risk (for protocol level)
        health['tvl_concentration'] = np.where(
            health.groupby('date')['tvl'].transform('sum') > 0,
            health['tvl'] / health.groupby('date')['tvl'].transform('sum'),
            0
        )
        
        # Replace infinite values with 0
        health = health.replace([np.inf, -np.inf], 0)
        
        # Debug: Print sample of health metrics
        latest_health = health[health['date'] == health['date'].max()]
        if len(latest_health) > 0:
            print(f"\nHealth metrics for latest date:")
            print(f"Average Fee APY: {latest_health['fee_apy'].mean() * 100:.2f}%")
            print(f"Average Efficiency Score: {latest_health['efficiency_score'].mean():.2f}")
        
        return health
    
    @with_progress("Creating summary statistics")
    def create_summary_stats(self, metrics: PoolMetrics) -> Dict:
        """Create high-level summary statistics"""
        
        latest_date = metrics.health_metrics['date'].max()
        latest_metrics = metrics.health_metrics[metrics.health_metrics['date'] == latest_date]
        
        # Remove protocol-wide row for pool stats
        pool_metrics = latest_metrics[latest_metrics['pool'] != 'ALL_POOLS']
        protocol_metrics = latest_metrics[latest_metrics['pool'] == 'ALL_POOLS']
        
        # Check if protocol_metrics exists and has data
        has_protocol_metrics = len(protocol_metrics) > 0
        
        # Handle NaN values with fallbacks
        if has_protocol_metrics:
            protocol_row = protocol_metrics.iloc[0]
            total_tvl = protocol_row.get('tvl', pool_metrics['tvl'].sum() if len(pool_metrics) > 0 else 0)
            daily_volume = protocol_row.get('volume', pool_metrics['volume'].sum() if len(pool_metrics) > 0 else 0)
            daily_fees = protocol_row.get('daily_fees', pool_metrics['daily_fees'].sum() if len(pool_metrics) > 0 else 0)
        else:
            total_tvl = pool_metrics['tvl'].sum() if len(pool_metrics) > 0 else 0
            daily_volume = pool_metrics['volume'].sum() if len(pool_metrics) > 0 else 0
            daily_fees = pool_metrics['daily_fees'].sum() if len(pool_metrics) > 0 else 0
        
        # Handle NaN values
        total_tvl = 0 if pd.isna(total_tvl) else total_tvl
        daily_volume = 0 if pd.isna(daily_volume) else daily_volume
        daily_fees = 0 if pd.isna(daily_fees) else daily_fees
        
        # Calculate average fee APY, handling NaN
        avg_fee_apy = 0
        if len(pool_metrics) > 0 and 'fee_apy' in pool_metrics.columns:
            avg_fee_apy = pool_metrics['fee_apy'].mean() * 100
            avg_fee_apy = 0 if pd.isna(avg_fee_apy) else avg_fee_apy
        
        summary = {
            'snapshot_date': latest_date,
            'protocol': {
                'total_tvl': total_tvl,
                'daily_volume': daily_volume,
                'daily_fees': daily_fees,
                'avg_fee_apy': avg_fee_apy,
                'total_pools': pool_metrics['pool'].nunique() if len(pool_metrics) > 0 else 0
            },
            'top_pools': {
                'by_tvl': pool_metrics.nlargest(3, 'tvl')[['pool', 'tvl']].to_dict('records') if len(pool_metrics) > 0 else [],
                'by_volume': pool_metrics.nlargest(3, 'volume')[['pool', 'volume']].to_dict('records') if len(pool_metrics) > 0 and 'volume' in pool_metrics.columns else [],
                'by_efficiency': pool_metrics.nlargest(3, 'efficiency_score')[['pool', 'efficiency_score']].to_dict('records') if len(pool_metrics) > 0 and 'efficiency_score' in pool_metrics.columns else []
            },
            'trends': {
                'tvl_7d_change': metrics.tvl_metrics.groupby('pool')['tvl'].apply(
                    lambda x: (x.iloc[-1] - x.iloc[-8]) / x.iloc[-8] * 100 if len(x) > 7 else 0
                ).mean() if len(metrics.tvl_metrics) > 0 else 0,
                'volume_7d_change': metrics.volume_metrics.groupby('pool')['volume'].apply(
                    lambda x: (x.iloc[-1] - x.iloc[-8]) / x.iloc[-8] * 100 if len(x) > 7 else 0
                ).mean() if len(metrics.volume_metrics) > 0 else 0
            }
        }
        
        return summary
    
    def process_all_data(self) -> PoolMetrics:
        """Main processing pipeline"""
        
        # Fetch data
        raw_data = self.fetch_pool_data()
        
        # Process transactions
        deposits, withdrawals, all_txns = self.process_transactions(
            raw_data['deposits'], 
            raw_data['withdrawals']
        )
        
        # Calculate metrics
        tvl_metrics = self.calculate_tvl_metrics(deposits, withdrawals)
        volume_metrics = self.calculate_volume_metrics(raw_data['volume'])
        fee_metrics = self.calculate_fee_metrics(raw_data['fees'], volume_metrics)
        user_metrics = self.calculate_user_metrics(deposits, withdrawals)
        health_metrics = self.calculate_health_metrics(tvl_metrics, volume_metrics, fee_metrics)
        
        # Create pool metrics object
        metrics = PoolMetrics(
            tvl_metrics=tvl_metrics,
            volume_metrics=volume_metrics,
            fee_metrics=fee_metrics,
            user_metrics=user_metrics,
            health_metrics=health_metrics,
            summary_stats={}
        )
        
        # Add summary stats
        metrics.summary_stats = self.create_summary_stats(metrics)
        
        return metrics
    
    @with_progress("Uploading metrics to BigQuery")
    def upload_to_bigquery(self, metrics: PoolMetrics):
        """Upload all metrics to BigQuery"""
        
        uploads = [
            (metrics.tvl_metrics, 'pool_tvl_metrics', 'date'),
            (metrics.volume_metrics, 'pool_volume_metrics', 'date'),
            (metrics.fee_metrics, 'pool_fee_metrics', 'date'),
            (metrics.user_metrics, 'pool_user_metrics', 'date'),
            (metrics.health_metrics, 'pool_health_metrics', 'date')
        ]
        
        for df, table_name, id_col in uploads:
            if df is not None and len(df) > 0:
                self.bq.update_table(df, 'analytics', table_name, id_col)
                ProgressIndicators.print_step(f"Uploaded {table_name}", "success")

    @with_progress("Saving metrics data as csv files")
    def save_to_csv(self, metrics: PoolMetrics):
        """Save all metrics to csv files"""
        
        uploads = [
            (metrics.tvl_metrics, 'pool_tvl_metrics',),
            (metrics.volume_metrics, 'pool_volume_metrics'),
            (metrics.fee_metrics, 'pool_fee_metrics'),
            (metrics.user_metrics, 'pool_user_metrics'),
            (metrics.health_metrics, 'pool_health_metrics')
        ]
        
        for df, table_name in uploads:
            if df is not None and len(df) > 0:
                self.save_to_csv(f'{table_name}_{datetime.today()}')
                ProgressIndicators.print_step(f"Uploaded {table_name}", "success")


def main():
    """Main execution function"""
    
    ProgressIndicators.print_header("POOL DATA PROCESSING PIPELINE V2")
    
    try:
        # Initialize
        load_dotenv(dotenv_path='../.env', override=True)
        pd.options.display.float_format = '{:.8f}'.format
        
        # Setup clients
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
        processor = PoolDataProcessor(bq)
        
        # Process all data
        metrics = processor.process_all_data()
        
        # Upload to BigQuery
        # processor.upload_to_bigquery(metrics)
        
        # Save data to csvs
        # processor.save_to_csv(metrics)
        
        # Print summary
        ProgressIndicators.print_summary_box(
            "📊 POOL METRICS SUMMARY",
            {
                "Total TVL": f"${metrics.summary_stats['protocol']['total_tvl']:,.2f}",
                "Daily Volume": f"${metrics.summary_stats['protocol']['daily_volume']:,.2f}",
                "Daily Fees": f"${metrics.summary_stats['protocol']['daily_fees']:,.2f}",
                "Avg Fee APY": f"{metrics.summary_stats['protocol']['avg_fee_apy']:.2f}%",
                "Active Pools": metrics.summary_stats['protocol']['total_pools'],
                "7D TVL Change": f"{metrics.summary_stats['trends']['tvl_7d_change']:.2f}%"
            }
        )
        
        # Save metrics for dashboard
        metrics.health_metrics.to_csv('pool_health_metrics.csv', index=False)
        pd.DataFrame([metrics.summary_stats]).to_json('pool_summary.json', orient='records')
        
        ProgressIndicators.print_header("✅ PROCESSING COMPLETE")
        return metrics
        
    except Exception as e:
        ProgressIndicators.print_step(f"Error: {str(e)}", "error")
        raise


if __name__ == "__main__":
    metrics = main()