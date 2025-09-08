import pandas as pd
from mezo.currency_utils import format_token_columns, format_musd_currency_columns, get_token_prices, format_pool_token_columns, add_pool_usd_conversions
from mezo.datetime_utils import format_datetimes
from mezo.clients import SubgraphClient
from mezo.currency_config import POOLS_MAP, TOKENS_ID_MAP, POOL_TOKEN0_MAP, POOL_TOKEN_PAIRS
from mezo.visual_utils import ProgressIndicators, ExceptionHandler
from mezo.queries import MUSDQueries

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

def add_usd_conversions_to_fee_data(df):
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
    df_with_usd['amount_usd_0'] = df_with_usd['amount0'] * df_with_usd['usd']
    df_with_usd['amount_usd_1'] = df_with_usd['amount1'] * df_with_usd['usd']

    return df_with_usd

def clean_fee_data(raw):
    """Clean and format swap fee data"""
    if not ExceptionHandler.validate_dataframe(raw, "Raw fee data", ['contractId_', 'timestamp_']):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy()
    df['pool'] = df['contractId_'].map(POOLS_MAP)
    df = format_datetimes(df, ['timestamp_'])

    df = format_pool_token_columns(df, 'contractId_', POOL_TOKEN_PAIRS)
    df = add_pool_usd_conversions(df, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)
    df['count'] = 1

    # df_with_usd = add_usd_conversions_to_fee_data(df)

    return df

# Get fees data
GET_FEES_FOR_SWAPS = """
query getSwapFees($skip: Int!) {
  fees(
  first: 1000
  orderBy: timestamp_
  orderDirection: desc
  skip: $skip
  ) {
    timestamp_
    sender
    amount0
    amount1
    contractId_
    transactionHash_
  }
}
"""

fees_data = SubgraphClient.get_subgraph_data(
    SubgraphClient.SWAPS_SUBGRAPH, 
    GET_FEES_FOR_SWAPS, 
    'fees'
)
fees_data.columns
# Clean the fees data
fees = clean_fee_data(fees_data)

# Get raw swap data from subgraph
raw_swap_data = SubgraphClient.get_subgraph_data(
    SubgraphClient.SWAPS_SUBGRAPH, 
    MUSDQueries.GET_SWAPS, 
    'swaps'
)

# Clean the swap data
swaps_df_clean = clean_swap_data(raw_swap_data)

# Merge dataframes
swaps_with_fees = pd.merge(swaps_df_clean, fees, how='left', on='transactionHash_')

swaps_with_fees_final = swaps_with_fees[['timestamp__x', 'sender_x', 'to', 'contractId__x', 'pool_x', 'pool_y', 
                                         'amount0In', 'amount0Out', 'amount1In', 'amount1Out', 'amount_usd_in', 'amount_usd_out', 
                                         'amount0', 'amount1',  'token0', 'token1', 'amount0_usd', 'amount1_usd', 'transactionHash_']]

swaps_fees_final = swaps_with_fees_final.dropna(subset=['amount0'])

col_map = {
    'timestamp__x': 'timestamp', 
    'sender_x': 'from',
    'to': 'to', 
    'contractId__x': 'contractId', 
    'pool_x': 'pool',
    'amount0In': 'amount0_in', 
    'amount0Out': 'amount0_out', 
    'amount1In': 'amount1_in', 
    'amount1Out': 'amount1_out', 
    'amount_usd_in': 'amount_usd_in',
    'amount_usd_out': 'amount_usd_out', 
    'amount0': 'fee0', 
    'amount1': 'fee1',
    'amount0_usd': 'fee0_usd', 
    'amount1_usd': 'fee1_usd', 
    'transactionHash_': 'transactionHash_'
}

swf = swaps_fees_final.rename(columns=col_map)