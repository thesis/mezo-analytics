import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, List, Optional, Tuple
import time

class MUSDPriceMonitor:
    """
    A comprehensive monitoring system for MUSD stablecoin price tracking
    """
    
    def __init__(self):
        self.endpoints = {
            'pools': 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-pools-mezo/1.0.0/gn',
            'token': 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-token/1.0.0/gn',
            'stability': 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-stability-pool/1.0.0/gn',
            'trove': 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-trove-manager/1.0.0/gn'
        }
        self.headers = {"Content-Type": "application/json"}
        self.musd_address = None  # Will be detected
        self.price_history = []
        
    def explore_subgraph(self, endpoint_name: str = 'pools') -> Dict:
        """
        Explore the schema of a subgraph to understand available entities
        """
        endpoint = self.endpoints.get(endpoint_name)
        if not endpoint:
            print(f"Unknown endpoint: {endpoint_name}")
            return {}
        
        introspection_query = """
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name
                            kind
                            ofType {
                                name
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json={"query": introspection_query}
            )
            
            if response.status_code == 200:
                data = response.json()
                schema = data.get('data', {}).get('__schema', {})
                
                # Filter for relevant entities
                entities = {}
                for type_def in schema.get('types', []):
                    name = type_def.get('name', '')
                    kind = type_def.get('kind', '')
                    
                    # Skip GraphQL internals and connections
                    if (not name.startswith('_') and 
                        kind == 'OBJECT' and 
                        name not in ['Query', 'Mutation', 'Subscription'] and
                        'Connection' not in name):
                        
                        fields = [f['name'] for f in type_def.get('fields', [])]
                        entities[name] = fields
                
                return entities
            
        except Exception as e:
            print(f"Error exploring {endpoint_name}: {e}")
        
        return {}
    
    def discover_price_sources(self) -> Dict[str, List]:
        """
        Discover available price data sources across all subgraphs
        """
        price_sources = {}
        
        # Common entity patterns for DEX/AMM pools
        pool_patterns = ['pool', 'pair', 'swap', 'sync', 'liquidity', 'reserve']
        
        for endpoint_name in self.endpoints.keys():
            print(f"\n🔍 Exploring {endpoint_name} subgraph...")
            entities = self.explore_subgraph(endpoint_name)
            
            relevant_entities = []
            for entity_name, fields in entities.items():
                entity_lower = entity_name.lower()
                
                # Check if entity might contain price data
                if any(pattern in entity_lower for pattern in pool_patterns):
                    relevant_entities.append({
                        'entity': entity_name,
                        'fields': fields,
                        'has_reserves': any('reserve' in f.lower() for f in fields),
                        'has_price': any('price' in f.lower() for f in fields),
                        'has_amounts': any('amount' in f.lower() for f in fields)
                    })
            
            if relevant_entities:
                price_sources[endpoint_name] = relevant_entities
                print(f"  ✅ Found {len(relevant_entities)} potential price entities")
            else:
                print(f"  ❌ No price-related entities found")
        
        return price_sources
    
    def query_entity(self, endpoint_name: str, entity: str, fields: List[str], 
                    limit: int = 100, order_by: str = 'timestamp') -> List[Dict]:
        """
        Query a specific entity from a subgraph
        """
        endpoint = self.endpoints.get(endpoint_name)
        if not endpoint:
            return []
        
        # Build query
        fields_str = '\n    '.join(fields)
        query = f"""
        {{
            {entity}(first: {limit}, orderBy: {order_by}, orderDirection: desc) {{
                {fields_str}
            }}
        }}
        """
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    return data['data'].get(entity, [])
        except Exception as e:
            print(f"Error querying {entity}: {e}")
        
        return []
    
    def detect_musd_pools(self) -> List[Dict]:
        """
        Detect pools that contain MUSD
        """
        musd_pools = []
        
        # Try to find pools/pairs
        pool_entities = ['pools', 'pairs']
        
        for entity in pool_entities:
            print(f"\n🔍 Searching for MUSD in {entity}...")
            
            # First, try to get pool data
            fields = ['id', 'token0', 'token1', 'reserve0', 'reserve1']
            pools = self.query_entity('pools', entity, fields)
            
            if pools:
                for pool in pools:
                    # Check if either token might be MUSD
                    token0 = pool.get('token0', '').lower()
                    token1 = pool.get('token1', '').lower()
                    
                    # Look for MUSD indicators
                    if 'musd' in token0 or 'musd' in token1:
                        musd_pools.append({
                            'pool_id': pool['id'],
                            'token0': token0,
                            'token1': token1,
                            'reserve0': pool.get('reserve0'),
                            'reserve1': pool.get('reserve1'),
                            'musd_is_token0': 'musd' in token0
                        })
                        print(f"  ✅ Found MUSD pool: {pool['id'][:10]}...")
        
        return musd_pools
    
    def calculate_price_from_reserves(self, reserve0: str, reserve1: str, 
                                     musd_is_token0: bool, decimals0: int = 18, 
                                     decimals1: int = 18) -> float:
        """
        Calculate MUSD price from pool reserves
        """
        try:
            # Convert reserves to float with decimal adjustment
            adj_reserve0 = float(reserve0) / (10 ** decimals0)
            adj_reserve1 = float(reserve1) / (10 ** decimals1)
            
            if adj_reserve0 == 0 or adj_reserve1 == 0:
                return 0
            
            if musd_is_token0:
                # Price of MUSD in terms of token1
                price = adj_reserve1 / adj_reserve0
            else:
                # Price of MUSD (token1) in terms of token0
                price = adj_reserve0 / adj_reserve1
            
            return price
        except Exception as e:
            print(f"Error calculating price: {e}")
            return 0
    
    def fetch_pool_history(self, pool_id: str, hours: int = 24) -> pd.DataFrame:
        """
        Fetch historical data for a specific pool
        """
        # Calculate timestamp for the time range
        timestamp_from = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        # Try sync events first (most common for price updates)
        query = f"""
        {{
            syncs(
                first: 1000
                orderBy: timestamp
                orderDirection: desc
                where: {{
                    pair: "{pool_id}"
                    timestamp_gte: {timestamp_from}
                }}
            ) {{
                id
                timestamp
                reserve0
                reserve1
                transaction {{
                    blockNumber
                }}
            }}
        }}
        """
        
        try:
            response = requests.post(
                self.endpoints['pools'],
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                data = response.json()
                syncs = data.get('data', {}).get('syncs', [])
                
                if syncs:
                    df = pd.DataFrame(syncs)
                    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
                    return df
        except Exception as e:
            print(f"Error fetching pool history: {e}")
        
        return pd.DataFrame()
    
    def fetch_swap_data(self, hours: int = 24) -> pd.DataFrame:
        """
        Fetch recent swap data to calculate implied prices
        """
        timestamp_from = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        query = f"""
        {{
            swaps(
                first: 1000
                orderBy: timestamp
                orderDirection: desc
                where: {{
                    timestamp_gte: {timestamp_from}
                }}
            ) {{
                id
                timestamp
                amount0In
                amount1In
                amount0Out
                amount1Out
                pair {{
                    token0 {{
                        symbol
                        decimals
                    }}
                    token1 {{
                        symbol
                        decimals
                    }}
                }}
            }}
        }}
        """
        
        try:
            response = requests.post(
                self.endpoints['pools'],
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                data = response.json()
                swaps = data.get('data', {}).get('swaps', [])
                
                if swaps:
                    df = pd.DataFrame(swaps)
                    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
                    
                    # Calculate implied price from each swap
                    df['implied_price'] = df.apply(self._calculate_swap_price, axis=1)
                    
                    return df[df['implied_price'] > 0]
        except Exception as e:
            print(f"Error fetching swap data: {e}")
        
        return pd.DataFrame()
    
    def _calculate_swap_price(self, swap: pd.Series) -> float:
        """
        Calculate implied price from a swap transaction
        """
        try:
            amount0_in = float(swap.get('amount0In', 0))
            amount1_in = float(swap.get('amount1In', 0))
            amount0_out = float(swap.get('amount0Out', 0))
            amount1_out = float(swap.get('amount1Out', 0))
            
            # Determine swap direction and calculate price
            if amount0_in > 0 and amount1_out > 0:
                # Token0 -> Token1 swap
                return amount1_out / amount0_in
            elif amount1_in > 0 and amount0_out > 0:
                # Token1 -> Token0 swap
                return amount1_in / amount0_out
        except:
            pass
        
        return 0
    
    def monitor_price_realtime(self, interval_seconds: int = 60, duration_minutes: int = 60):
        """
        Monitor MUSD price in real-time
        """
        print("\n🚀 Starting real-time MUSD price monitoring...")
        
        # First, detect MUSD pools
        musd_pools = self.detect_musd_pools()
        
        if not musd_pools:
            print("❌ No MUSD pools found. Trying alternative methods...")
            return self._monitor_alternative_sources(interval_seconds, duration_minutes)
        
        # Monitor the first MUSD pool found
        pool = musd_pools[0]
        print(f"📊 Monitoring pool: {pool['pool_id'][:10]}...")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        price_data = []
        
        while datetime.now() < end_time:
            # Fetch current pool state
            query = f"""
            {{
                pool(id: "{pool['pool_id']}") {{
                    reserve0
                    reserve1
                    totalSupply
                    token0Price
                    token1Price
                }}
            }}
            """
            
            try:
                response = requests.post(
                    self.endpoints['pools'],
                    headers=self.headers,
                    json={"query": query}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    pool_data = data.get('data', {}).get('pool')
                    
                    if pool_data:
                        price = self.calculate_price_from_reserves(
                            pool_data['reserve0'],
                            pool_data['reserve1'],
                            pool['musd_is_token0']
                        )
                        
                        price_data.append({
                            'timestamp': datetime.now(),
                            'price': price,
                            'reserve0': pool_data['reserve0'],
                            'reserve1': pool_data['reserve1']
                        })
                        
                        # Print status
                        deviation = (price - 1.0) * 100
                        status = "🟢" if abs(deviation) < 2 else "🟡" if abs(deviation) < 5 else "🔴"
                        print(f"{status} {datetime.now().strftime('%H:%M:%S')} - Price: ${price:.4f} (Deviation: {deviation:+.2f}%)")
                        
                        # Alert if significant depeg
                        if abs(deviation) > 2:
                            print(f"⚠️ ALERT: MUSD depegged by {deviation:+.2f}%!")
            
            except Exception as e:
                print(f"Error fetching price: {e}")
            
            time.sleep(interval_seconds)
        
        return pd.DataFrame(price_data)
    
    def _monitor_alternative_sources(self, interval_seconds: int, duration_minutes: int):
        """
        Monitor price from alternative sources (transfers, mints, etc.)
        """
        print("🔍 Attempting to calculate price from MUSD transfers and mints...")
        
        # Query MUSD token transfers to understand volume and potential price indicators
        query = """
        {
            transfers(first: 100, orderBy: timestamp, orderDirection: desc) {
                from
                to
                value
                timestamp
            }
        }
        """
        
        try:
            response = requests.post(
                self.endpoints['token'],
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                data = response.json()
                transfers = data.get('data', {}).get('transfers', [])
                
                if transfers:
                    df = pd.DataFrame(transfers)
                    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
                    df['value'] = df['value'].astype(float) / 1e18  # Assuming 18 decimals
                    
                    print(f"✅ Found {len(transfers)} recent transfers")
                    print(f"📊 Total volume: {df['value'].sum():.2f} MUSD")
                    
                    return df
        except Exception as e:
            print(f"Error: {e}")
        
        return pd.DataFrame()
    
    def generate_price_report(self, price_data: pd.DataFrame) -> Dict:
        """
        Generate comprehensive price analysis report
        """
        if price_data.empty:
            return {"error": "No price data available"}
        
        current_price = price_data['price'].iloc[-1]
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "current_price": current_price,
            "current_deviation_%": (current_price - 1.0) * 100,
            "avg_price": price_data['price'].mean(),
            "min_price": price_data['price'].min(),
            "max_price": price_data['price'].max(),
            "volatility_%": price_data['price'].std() * 100,
            "samples": len(price_data),
            "depeg_events": ((abs(price_data['price'] - 1.0) > 0.02)).sum(),
            "health_status": self._calculate_health_status(price_data)
        }
        
        return report
    
    def _calculate_health_status(self, price_data: pd.DataFrame) -> str:
        """
        Calculate overall health status of MUSD peg
        """
        current_price = price_data['price'].iloc[-1]
        deviation = abs(current_price - 1.0)
        volatility = price_data['price'].std()
        
        if deviation < 0.01 and volatility < 0.005:
            return "EXCELLENT - Stable peg"
        elif deviation < 0.02 and volatility < 0.01:
            return "GOOD - Minor fluctuations"
        elif deviation < 0.05 and volatility < 0.02:
            return "WARNING - Moderate volatility"
        else:
            return "CRITICAL - Significant depeg risk"
    
    def visualize_price_data(self, price_data: pd.DataFrame):
        """
        Create visualization of price data
        """
        if price_data.empty:
            print("No data to visualize")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('MUSD Price Monitoring Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Price over time
        ax1 = axes[0, 0]
        ax1.plot(price_data['timestamp'], price_data['price'], 'b-', linewidth=2)
        ax1.axhline(y=1.0, color='g', linestyle='--', label='$1.00 Peg')
        ax1.axhline(y=1.02, color='r', linestyle=':', alpha=0.5)
        ax1.axhline(y=0.98, color='r', linestyle=':', alpha=0.5)
        ax1.fill_between(price_data['timestamp'], 0.98, 1.02, alpha=0.1, color='green')
        ax1.set_title('MUSD Price Over Time')
        ax1.set_ylabel('Price (USD)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Deviation distribution
        ax2 = axes[0, 1]
        deviation = (price_data['price'] - 1.0) * 100
        ax2.hist(deviation, bins=30, color='skyblue', edgecolor='black')
        ax2.axvline(x=0, color='g', linestyle='-', linewidth=2)
        ax2.set_title('Price Deviation Distribution')
        ax2.set_xlabel('Deviation from Peg (%)')
        ax2.set_ylabel('Frequency')
        
        # 3. Rolling volatility
        ax3 = axes[1, 0]
        if len(price_data) > 10:
            rolling_vol = price_data['price'].rolling(window=min(10, len(price_data))).std() * 100
            ax3.plot(price_data['timestamp'], rolling_vol, 'purple', linewidth=2)
            ax3.fill_between(price_data['timestamp'], 0, rolling_vol, alpha=0.3, color='purple')
        ax3.set_title('Rolling Volatility (10-period)')
        ax3.set_ylabel('Volatility (%)')
        ax3.grid(True, alpha=0.3)
        
        # 4. Price statistics box
        ax4 = axes[1, 1]
        ax4.axis('off')
        
        report = self.generate_price_report(price_data)
        stats_text = f"""
        Current Price: ${report['current_price']:.4f}
        Deviation: {report['current_deviation_%']:+.2f}%
        Average Price: ${report['avg_price']:.4f}
        Min Price: ${report['min_price']:.4f}
        Max Price: ${report['max_price']:.4f}
        Volatility: {report['volatility_%']:.2f}%
        Depeg Events: {report['depeg_events']}
        
        Status: {report['health_status']}
        """
        
        ax4.text(0.1, 0.5, stats_text, fontsize=12, verticalalignment='center',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        return fig


# Example usage
if __name__ == "__main__":
    # Initialize the monitor
    monitor = MUSDPriceMonitor()
    
    print("="*60)
    print("MUSD PRICE MONITORING SYSTEM")
    print("="*60)
    
    # 1. Discover available price sources
    print("\n📡 Discovering price sources...")
    price_sources = monitor.discover_price_sources()
    
    # 2. Detect MUSD pools
    print("\n🔍 Detecting MUSD pools...")
    musd_pools = monitor.detect_musd_pools()
    
    if musd_pools:
        print(f"\n✅ Found {len(musd_pools)} MUSD pools")
        for pool in musd_pools:
            print(f"  - Pool: {pool['pool_id'][:10]}... (MUSD is token{'0' if pool['musd_is_token0'] else '1'})")
    
    # 3. Fetch historical data
    print("\n📊 Fetching historical price data...")
    swap_data = monitor.fetch_swap_data(hours=24)
    
    if not swap_data.empty:
        print(f"✅ Found {len(swap_data)} swap transactions")
        
        # 4. Generate report
        report = monitor.generate_price_report(swap_data)
        print("\n📈 Price Report:")
        for key, value in report.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.4f}")
            else:
                print(f"  {key}: {value}")
    
    # 5. Optional: Start real-time monitoring
    # Uncomment to enable real-time monitoring
    # print("\n🚀 Starting real-time monitoring (5 minutes)...")
    # realtime_data = monitor.monitor_price_realtime(interval_seconds=30, duration_minutes=5)
    # 
    # if not realtime_data.empty:
    #     fig = monitor.visualize_price_data(realtime_data)
    #     plt.show()