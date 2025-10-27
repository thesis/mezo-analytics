import pandas as pd
from web3 import Web3
from datetime import datetime
from typing import List, Dict, Optional
from mezo.clients import Web3Client, SubgraphClient
import time
from mezo.queries import BridgeQueries


class BridgeFeesFetcher:
    """Fetches transaction fees from the Mezo bridge contract"""

    def __init__(self, contract_name: str = 'bridge', rate_limit_delay: float = 0.1):
        """Initialize Web3 connection and load contract

        Args:
            contract_name: Name of the contract to load
            rate_limit_delay: Delay in seconds between RPC calls (default: 0.1)
        """
        self.web3_client = Web3Client(contract_name)
        self.w3 = self.web3_client.w3
        self.contract = self.web3_client.load_contract()
        self.rate_limit_delay = rate_limit_delay

        print(f"✓ Connected to Mezo network")
        print(f"  Latest block: {self.w3.eth.block_number}")
        print(f"  Contract: {self.contract.address}")
        print(f"  Rate limit delay: {rate_limit_delay}s between calls")

    def get_transaction_fee(self, tx_hash: str, retry_count: int = 3):
        """
        Get transaction fee details for a given transaction hash with retry logic

        Args:
            tx_hash: Transaction hash
            retry_count: Number of retries on rate limit errors (default: 3)

        Returns: Dictionary with transaction fee details
        """

        for attempt in range(retry_count):
            try:
                # Add delay to respect rate limits
                if attempt > 0:
                    backoff_delay = self.rate_limit_delay * (2 ** attempt)  # Exponential backoff
                    print(f"  Retry {attempt + 1}/{retry_count} after {backoff_delay}s delay...")
                    time.sleep(backoff_delay)
                else:
                    time.sleep(self.rate_limit_delay)

                # Get transaction receipt and transaction details
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                time.sleep(self.rate_limit_delay)

                tx = self.w3.eth.get_transaction(tx_hash)
                time.sleep(self.rate_limit_delay)

                # Calculate transaction fee
                gas_used = receipt['gasUsed']
                gas_price = tx['gasPrice']
                tx_fee_wei = gas_used * gas_price
                tx_fee_eth = self.w3.from_wei(tx_fee_wei, 'ether')

                # Get block timestamp
                block = self.w3.eth.get_block(receipt['blockNumber'])
                timestamp = datetime.fromtimestamp(block['timestamp'])

                return {
                    'transaction_hash': tx_hash,
                    'block_number': receipt['blockNumber'],
                    'timestamp': timestamp,
                    'from_address': receipt['from'],
                    'to_address': receipt['to'],
                    'gas_used': gas_used,
                    'gas_price': gas_price,
                    'gas_price_gwei': float(self.w3.from_wei(gas_price, 'gwei')),
                    'transaction_fee_wei': tx_fee_wei,
                    'transaction_fee_eth': float(tx_fee_eth),
                    'status': receipt['status']
                }
            except Exception as e:
                error_msg = str(e).lower()
                if 'rate limit' in error_msg or 'too many requests' in error_msg or '429' in error_msg:
                    if attempt < retry_count - 1:
                        continue  # Retry
                    else:
                        print(f"Rate limit error after {retry_count} retries for {tx_hash}: {str(e)}")
                        return None
                else:
                    print(f"Error fetching fee for {tx_hash}: {str(e)}")
                    return None

        return None

    def get_fees_for_transactions(self, tx_hashes: List[str]):
        """
        Get transaction fees for a list of transaction hashes
        Returns: DataFrame with transaction fee details
        """

        print(f"\n📊 Fetching fees for {len(tx_hashes)} transactions...")

        results = []
        total = len(tx_hashes)

        for i, tx_hash in enumerate(tx_hashes, 1):
            if i % 10 == 0 or i == total:
                print(f"  Processing {i}/{total}...", end='\r')

            fee_data = self.get_transaction_fee(tx_hash)
            if fee_data:
                results.append(fee_data)

        print(f"\n✓ Successfully processed {len(results)} transactions")

        return pd.DataFrame(results)

fetcher = BridgeFeesFetcher('bridge', rate_limit_delay=.1)

raw_withdrawals = SubgraphClient.get_subgraph_data(
    SubgraphClient.MEZO_BRIDGE_OUT_SUBGRAPH,
    BridgeQueries.GET_NATIVE_WITHDRAWALS,
    'assetsUnlockeds'
)

df_events = raw_withdrawals

tx_hashes =  df_events['transactionHash_'].to_list()
df_fees = fetcher.get_fees_for_transactions(tx_hashes)

df_fees[[
    'gas_used', 'gas_price', 'gas_price_gwei', 
    'transaction_fee_wei', 'transaction_fee_eth'
]]