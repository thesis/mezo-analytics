import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from mezo.clients import SubgraphClient
from mezo.currency_config import MEZO_TOKEN_ADDRESSES, POOLS_MAP, TIGRIS_MAP
from mezo.queries import PoolQueries, MUSDQueries
from mezo.data_utils import flatten_json_column

# Your MUSD pools subgraph endpoint
SubgraphClient.POOLS_SUBGRAPH
SubgraphClient.TIGRIS_POOLS_SUBGRAPH
SubgraphClient.SUBGRAPH_HEADERS

# Common stablecoin addresses (you'll need to update these with actual addresses)
MEZO_TOKEN_ADDRESSES

# Get pool metadata from the Tigris subgraph

POOLS_DATA = """
query getPoolData($skip: Int!) {
  pools(
  skip: $skip
) {
    name
    token0 {
      symbol
    }
    token1 {
      symbol
    }
    id
  }
}
"""

metadata = SubgraphClient.get_subgraph_data(SubgraphClient.TIGRIS_POOLS_SUBGRAPH, POOLS_DATA, 'pools')
pools = pd.DataFrame(metadata)

pools_flat = flatten_json_column(pools, 'token0')
pools_flat = flatten_json_column(pools_flat, 'token1')

"""Get sync events from the MUSD pools subgraph"""

GET_RESERVE_DATA = """
query MyQuery($skip: Int!) {
  syncs(
    first: 40
    orderBy: timestamp_
    orderDirection: desc
    skip: $skip
  ) {
    timestamp_
    reserve0
    reserve1
    contractId_
    transactionHash_
  }
}
"""

sync_data = SubgraphClient.get_subgraph_data(SubgraphClient.POOLS_SUBGRAPH, GET_RESERVE_DATA, 'syncs')

sync_data.head()
sync_data.columns

"""
Combine pool metadata with sync data to create a single dataframe
with all necessary fields from pools_data and pool
"""

# Map contractId_ to pool names using POOLS_MAP

sync_data['pool'] = sync_data['contractId_'].map(POOLS_MAP)
sync_data_trim = sync_data.loc[sync_data['pool'] == 'musdc_musd_pool']

# Map Tigris pool names to our standardized names
pools_flat.head()

pools_flat['pool'] = pools_flat['name'].map(TIGRIS_MAP)
pools_flat_trim = pools_flat.loc[pools_flat['pool'].notna()]
pools_flat_trim = pools_flat_trim.loc[pools_flat_trim['pool'] == 'musdc_musd_pool']
# Join the datasets on pool name
combined_df = pd.merge(
    sync_data_trim,
    pools_flat_trim,
    on='pool',
    how='inner',
    suffixes=('_sync', '_meta')
)

# Add calculated fields
combined_df['reserve0_float'] = pd.to_numeric(combined_df['reserve0'], errors='coerce')
combined_df['reserve1_float'] = pd.to_numeric(combined_df['reserve1'], errors='coerce')

# Adjust reserves for decimals
from decimal import Decimal

def convert(x, token):
    if pd.isnull(x):
        return 0
    if token in {"USDC", "USDT", "mUSDC", "mUSDT"}:
        scale = Decimal("1e6")
    elif token in {"WBTC", "FBTC", "cbBTC", "swBTC"}:
        scale = Decimal("1e8")
    else:
        scale = Decimal("1e18")
    return float((Decimal(x) / scale).normalize())

combined_df['adjusted_reserve0'] = combined_df['reserve0_float'] / 1e6
combined_df['adjusted_reserve1'] = combined_df['reserve1_float'] / 1e18

combined_df.head()
combined_df['musd_price'] = combined_df['adjusted_reserve0'] / combined_df['adjusted_reserve1']


combined_df.columns

combined_df_trim = combined_df[[
    'timestamp_', 'adjusted_reserve0', 'adjusted_reserve1', 'musd_price', 'pool', 'name', 
    'token0_symbol', 'token1_symbol', 'reserve0_float', 'reserve1_float', 'contractId_', 
]]

combined_df_trim = combined_df_trim.rename(columns={
    'adjusted_reserve0': 'musd_reserves',
    'adjusted_reserve1': 'pair_reserves'
    })


def get_musd_price_summary() -> dict:
    """
    Get a comprehensive summary of MUSD price data
    """
    pool_prices_df = combined_df_trim
    
    if pool_prices_df.empty:
        return {
            'pool_prices': None,
            'overall': None,
            'status': 'No price data available'
        }
    
    # Calculate weighted average price (by reserves)
    total_musd_reserves = pool_prices_df['musd_reserves'].sum()
    if total_musd_reserves > 0:
        weighted_price = (pool_prices_df['musd_price'] * pool_prices_df['musd_reserves']).sum() / total_musd_reserves
    else:
        weighted_price = pool_prices_df['musd_price'].mean()
        
    # Calculate deviation from $1 peg
    deviation_percent = (weighted_price - 1.0) * 100
    
    return {
        'pool_prices': {
            'pools': pool_prices_df.to_dict('records'),
            'count': len(pool_prices_df)
        },
        'overall': {
            'best_estimate_price': weighted_price,
            'deviation_from_peg_%': deviation_percent,
            'status': _get_status(deviation_percent)
        },
        'timestamp': datetime.now().isoformat()
    }

def _get_status(deviation_percent: float) -> str:
    """Get health status based on deviation"""
    abs_dev = abs(deviation_percent)
    if abs_dev < 0.5:
        return "✅ HEALTHY - Strong peg"
    elif abs_dev < 1.0:
        return "✅ GOOD - Minor deviation"
    elif abs_dev < 2.0:
        return "⚠️ WARNING - Notable deviation"
    elif abs_dev < 5.0:
        return "🟠 CONCERN - Significant deviation"
    else:
        return "🔴 CRITICAL - Major depeg"


# ===========================
# USAGE EXAMPLE
# ===========================

def main():
    """Example usage"""
    
    # Get combined pools data
    print("\n📊 Getting combined pools data...")
    combined_data = combined_df_trim
    
    if not combined_data.empty:
        print(f"✅ Combined data shape: {combined_data.shape}")
        print("\nColumns available:")
        for col in sorted(combined_data.columns):
            print(f"  • {col}")
    
    # Get comprehensive price analysis
    print("\n💰 Calculating MUSD prices...")
    summary = get_musd_price_summary()
    
    if summary['pool_prices']:
        df_pools = pd.DataFrame(summary['pool_prices']['pools'])
        print("\n📊 Pool Prices DataFrame:")
        print(df_pools[['pool', 'musd_price', 'musd_reserves']].to_string(index=False))
        
        # Print overall summary
        if summary['overall']:
            overall = summary['overall']
            print(f"\n📈 Overall MUSD Price: ${overall['best_estimate_price']:.4f}")
            print(f"📊 Deviation from peg: {overall['deviation_from_peg_%']:+.2f}%")
            print(f"🎯 Status: {overall['status']}")
    else:
        print("⚠️ No price data available")
    
    return summary

if __name__ == "__main__":
    # Run the price calculator
    results = main()