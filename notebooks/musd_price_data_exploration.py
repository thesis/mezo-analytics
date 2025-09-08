#!/usr/bin/env python3
"""
Simple MUSD Price Calculator
Calculate MUSD price from pools and swaps data
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from mezo.clients import SubgraphClient
from mezo.currency_config import MEZO_TOKEN_ADDRESSES, POOLS_MAP, TIGRIS_MAP
from mezo.queries import PoolQueries, MUSDQueries

class SimpleMUSDPriceCalculator:
    def __init__(self):
        # Your MUSD pools subgraph endpoint
        self.pools_endpoint = SubgraphClient.POOLS_SUBGRAPH
        self.tigris_endpoint = SubgraphClient.TIGRIS_POOLS_SUBGRAPH
        self.headers = SubgraphClient.SUBGRAPH_HEADERS
        
        # Common stablecoin addresses (you'll need to update these with actual addresses)
        self.stablecoins = MEZO_TOKEN_ADDRESSES

    def get_pools_data(self):
        """Get pool metadata from the Tigris subgraph"""
        POOLS_DATA = """
        query getPoolsData {
          pools(first: 100) {
            name
            token0 {
              symbol
              decimals
            }
            token1 {
              symbol
              decimals
            }
            id
            reserve0
            reserve1
            totalSupply
          }
        }
        """
        result = SubgraphClient.get_subgraph_data(self.tigris_endpoint, POOLS_DATA, 'pools')
        
        if result is None or result.empty:
            print("No  pools data found")
            return pd.DataFrame()
    
        pools = result['data']['pools']
        print(pools.head())
        
        # Flatten the nested structure 
        flattened_pools = []
        for pool in pools:
            flattened_pool = {
                'pool_id': pool['id'],
                'pool_name': pool['name'],
                'token0_symbol': pool['token0']['symbol'],
                'token1_symbol': pool['token1']['symbol'],
                'token0_decimals': pool['token0'].get('decimals', 18),
                'token1_decimals': pool['token1'].get('decimals', 18),
                'reserve0': pool.get('reserve0', '0'),
                'reserve1': pool.get('reserve1', '0'),
                'total_supply': pool.get('totalSupply', '0')
            }
            flattened_pools.append(flattened_pool)

        print(flattened_pool.head())
            
        return pd.DataFrame(flattened_pools)
    
    def get_sync_data(self) -> pd.DataFrame:
        """Get sync events from the MUSD pools subgraph"""
        sync_data = SubgraphClient.get_subgraph_data(self.pools_endpoint, MUSDQueries.GET_MUSD_PRICE, 'syncs')
        
        if sync_data is None or sync_data.empty:
            print("No sync data found")
            return pd.DataFrame()
        
        print(sync_data.head())

        return sync_data
    
    def get_combined_pools_data(self) -> pd.DataFrame:
        """
        Combine pool metadata with sync data to create a single dataframe
        with all necessary fields from pools_data and pool
        """
        # Get pool metadata from Tigris subgraph
        pools_data = self.get_pools_data()
        if pools_data.empty:
            return pd.DataFrame()
            
        # Get sync data from MUSD pools subgraph
        sync_data = self.get_sync_data()
        if sync_data.empty:
            return pd.DataFrame()
            
        # Map contractId_ to pool names using POOLS_MAP
        sync_data['pool_name_mapped'] = sync_data['contractId_'].map(
            {addr.lower(): name for addr, name in POOLS_MAP.items()}
        )
        
        # Map Tigris pool names to our standardized names
        pools_data['pool_name_mapped'] = pools_data['pool_name'].map(TIGRIS_MAP)
        
        # Join the datasets on pool name
        combined_df = pd.merge(
            sync_data,
            pools_data,
            on='pool_name_mapped',
            how='inner',
            suffixes=('_sync', '_meta')
        )

        combined_df.loc[combined_df['pool_name_mapped'] != 'btc_musd_pool']
        
        # Add calculated fields
        combined_df['reserve0_float'] = pd.to_numeric(combined_df['reserve0'], errors='coerce')
        combined_df['reserve1_float'] = pd.to_numeric(combined_df['reserve1'], errors='coerce')
        combined_df['token0_decimals'] = pd.to_numeric(combined_df['token0_decimals'], errors='coerce')
        combined_df['token1_decimals'] = pd.to_numeric(combined_df['token1_decimals'], errors='coerce')
        
        # Adjust reserves for decimals
        combined_df['adjusted_reserve0'] = combined_df['reserve0_float'] / (10 ** combined_df['token0_decimals'])
        combined_df['adjusted_reserve1'] = combined_df['reserve1_float'] / (10 ** combined_df['token1_decimals'])
        
        print(combined_df.head())
        
        return combined_df
    
    # ===========================
    # METHOD 1: PRICE FROM POOLS
    # ===========================
    
    def get_price_from_pools(self) -> pd.DataFrame:
        """
        Calculate MUSD price from liquidity pool reserves using combined data
        
        How it works:
        1. Get combined pool metadata and sync data
        2. Filter for pools containing MUSD
        3. Calculate price as ratio of adjusted reserves
        """
        
        combined_df = self.get_combined_pools_data()
        if combined_df.empty:
            print("No combined pool data found")
            return pd.DataFrame()
        
        price_data = []
        
        for _, row in combined_df.iterrows():
            # Check if pool contains MUSD
            token0_symbol = str(row['token0_symbol']).lower()
            token1_symbol = str(row['token1_symbol']).lower()
            
            # Skip if neither token is MUSD
            if 'musd' not in token0_symbol and 'musd' not in token1_symbol:
                continue
            
            # Determine which token is MUSD
            musd_is_token0 = 'musd' in token0_symbol
            
            # Get adjusted reserves
            adjusted_reserve0 = row['adjusted_reserve0']
            adjusted_reserve1 = row['adjusted_reserve1']
            
            # Calculate price
            if adjusted_reserve0 > 0 and adjusted_reserve1 > 0:
                if musd_is_token0:
                    # MUSD is token0, price in terms of token1
                    musd_price = adjusted_reserve1 / adjusted_reserve0
                    pair_token = token1_symbol
                else:
                    # MUSD is token1, price in terms of token0
                    musd_price = adjusted_reserve0 / adjusted_reserve1
                    pair_token = token0_symbol
                
                price_data.append({
                    'pool_id': str(row['contractId_'])[:10] + '...',
                    'pool_name': row['pool_name'],
                    'musd_price': musd_price,
                    'paired_with': pair_token.upper(),
                    'musd_reserves': adjusted_reserve0 if musd_is_token0 else adjusted_reserve1,
                    'pair_reserves': adjusted_reserve1 if musd_is_token0 else adjusted_reserve0,
                    'timestamp': row['timestamp_'],
                    'block_number': row.get('block_number', ''),
                    'transaction_hash': row.get('transactionHash_', ''),
                    'method': 'pool_reserves'
                })
                
                print(f"✅ Pool {row['pool_name']}: MUSD/{pair_token.upper()} = ${musd_price:.4f}")
        
        return pd.DataFrame(price_data)
    
    def get_musd_price_summary(self) -> dict:
        """
        Get a comprehensive summary of MUSD price data
        """
        pool_prices_df = self.get_price_from_pools()
        
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
                'status': self._get_status(deviation_percent)
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_status(self, deviation_percent: float) -> str:
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
    
    # Initialize calculator
    calculator = SimpleMUSDPriceCalculator()
    
    # Get combined pools data
    print("\n📊 Getting combined pools data...")
    combined_data = calculator.get_combined_pools_data()
    
    if not combined_data.empty:
        print(f"✅ Combined data shape: {combined_data.shape}")
        print("\nColumns available:")
        for col in sorted(combined_data.columns):
            print(f"  • {col}")
        
        # Save combined data to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'musd_combined_pools_data_{timestamp}.csv'
        combined_data.to_csv(filename, index=False)
        print(f"✅ Saved combined data to {filename}")
    
    # Get comprehensive price analysis
    print("\n💰 Calculating MUSD prices...")
    summary = calculator.get_musd_price_summary()
    
    if summary['pool_prices']:
        df_pools = pd.DataFrame(summary['pool_prices']['pools'])
        print("\n📊 Pool Prices DataFrame:")
        print(df_pools[['pool_name', 'musd_price', 'paired_with', 'musd_reserves']].to_string(index=False))
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        price_filename = f'musd_pool_prices_{timestamp}.csv'
        df_pools.to_csv(price_filename, index=False)
        print(f"✅ Saved pool prices to {price_filename}")
        
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