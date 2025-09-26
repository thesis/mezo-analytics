from decimal import Decimal
import os
import requests
# from dotenv import load_dotenv
from mezo.currency_config import TOKENS_ID
import pandas as pd

# load_dotenv(dotenv_path="../.env", override=True)
COINGECKO_KEY = os.getenv('COINGECKO_KEY')

def format_currency_columns(df, cols, asset):
    def convert(x, token):
        if pd.isnull(x):
            return 0
        if token in {"USDC", "USDT"}:
            scale = Decimal("1e6")
        elif token in {"WBTC", "FBTC", "cbBTC", "swBTC"}:
            scale = Decimal("1e8")
        else:
            scale = Decimal("1e18")
        return float((Decimal(x) / scale).normalize())

    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df.apply(lambda row: convert(row[col], row[asset]), axis=1)
    
    return df

def format_token_columns(df, cols, asset):
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

    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df.apply(lambda row: convert(row[col], row[asset]), axis=1)
    
    return df

def format_pool_token_columns(df, pool_column, pool_token_pairs):
    """
    Format token columns for pool data using token pair mappings
    
    Args:
        df: DataFrame containing pool data
        pool_column: Name of column containing pool identifiers (contractId_)
        pool_token_pairs: Dictionary mapping pool IDs to token0/token1 pairs
    
    Returns:
        DataFrame with formatted token columns and token0/token1 mappings
    """
    def convert(x, token):
        if pd.isnull(x):
            return 0
        if token in {"USDC", "USDT", "mUSDC", "mUSDT"}:
            scale = Decimal("1e6")
        elif token in {"BTC", "MUSD"}:
            scale = Decimal("1e18")
        else:
            scale = Decimal("1e18")  # Default for everything else
        return float((Decimal(x) / scale).normalize())
    
    df_result = df.copy()
    
    # Add token0 and token1 columns based on pool mapping
    df_result['token0'] = df_result[pool_column].map(lambda x: pool_token_pairs.get(x, {}).get('token0'))
    df_result['token1'] = df_result[pool_column].map(lambda x: pool_token_pairs.get(x, {}).get('token1'))
    
    # Format amount0 columns using token0
    amount0_cols = [col for col in df_result.columns if col.startswith('amount0')]
    for col in amount0_cols:
        df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
        df_result[col] = df_result.apply(lambda row: convert(row[col], row['token0']), axis=1)
    
    # Format amount1 columns using token1  
    amount1_cols = [col for col in df_result.columns if col.startswith('amount1')]
    for col in amount1_cols:
        df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
        df_result[col] = df_result.apply(lambda row: convert(row[col], row['token1']), axis=1)
    
    # Format totalVolume columns if they exist
    if 'totalVolume0' in df_result.columns:
        df_result['totalVolume0'] = pd.to_numeric(df_result['totalVolume0'], errors='coerce')
        df_result['totalVolume0'] = df_result.apply(lambda row: convert(row['totalVolume0'], row['token0']), axis=1)
    
    if 'totalVolume1' in df_result.columns:
        df_result['totalVolume1'] = pd.to_numeric(df_result['totalVolume1'], errors='coerce')
        df_result['totalVolume1'] = df_result.apply(lambda row: convert(row['totalVolume1'], row['token1']), axis=1)
    
    return df_result

def format_musd_currency_columns(df, cols):
    df[cols] = df[cols].apply(lambda col: pd.to_numeric(col, errors='coerce').apply(
        lambda x: float((Decimal(x) / Decimal("1e18")).normalize()) if pd.notnull(x) else 0
    ))
    return df

def replace_token_labels(df, token_map):
    """
    Replaces values in the 'token' column using the TOKEN_MAP dictionary in currency_config.

    Parameters:
        df: The DataFrame containing a 'token' column.
        token_map (dict): Dictionary mapping token addresses to human-readable labels.

    Returns:
        pd.DataFrame: Updated DataFrame with replaced token values.
    """
    # Normalize the token column to lowercase for matching
    df['token'] = df['token'].str.lower()

    # Normalize the token_map keys to lowercase
    normalized_map = {k.lower(): v for k, v in token_map.items()}

    df['token'] = df['token'].replace(normalized_map)
    return df

def get_token_prices():
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {'ids': TOKENS_ID, 'vs_currencies': 'usd'}
    headers = {'x-cg-demo-api-key': COINGECKO_KEY}

    response = requests.get(url, params = params)

    data = response.json()
    df = pd.DataFrame(data)

    return df

def get_token_price(token_id):
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {'ids': token_id, 'vs_currencies': 'usd'}
    headers = {'x-cg-demo-api-key': COINGECKO_KEY}

    response = requests.get(url, params = params)

    data = response.json()
    price = data[token_id]['usd']
    
    return price

def add_pool_usd_conversions(df, pool_column, pool_token_pairs, tokens_id_map):
    """
    Add USD conversions for pool data with token0/token1 pairs
    
    Args:
        df: DataFrame containing pool data
        pool_column: Name of column containing pool identifiers (contractId_)
        pool_token_pairs: Dictionary mapping pool IDs to token0/token1 pairs
        tokens_id_map: Dictionary mapping tokens to CoinGecko IDs
    
    Returns:
        DataFrame with USD conversion columns added
    """
    if pool_column not in df.columns:
        raise ValueError(f"Column '{pool_column}' not found in DataFrame")
    
    # Get token prices
    prices = get_token_prices()
    if prices is None or prices.empty:
        raise ValueError("No token prices received from API")
    
    token_usd_prices = prices.T.reset_index()
    df_result = df.copy()
    
    # Add token0 and token1 columns based on pool mapping
    df_result['token0'] = df_result[pool_column].map(lambda x: pool_token_pairs.get(x, {}).get('token0'))
    df_result['token1'] = df_result[pool_column].map(lambda x: pool_token_pairs.get(x, {}).get('token1'))
    
    # Create separate rows for token0 and token1 conversions
    df_result['token0_index'] = df_result['token0'].map(tokens_id_map)
    df_result['token1_index'] = df_result['token1'].map(tokens_id_map)
    
    # Merge token0 prices
    token0_prices = token_usd_prices.rename(columns={'index': 'token0_index', 'usd': 'token0_usd_rate'})
    df_result = pd.merge(df_result, token0_prices[['token0_index', 'token0_usd_rate']], on='token0_index', how='left')
    
    # Merge token1 prices
    token1_prices = token_usd_prices.rename(columns={'index': 'token1_index', 'usd': 'token1_usd_rate'})
    df_result = pd.merge(df_result, token1_prices[['token1_index', 'token1_usd_rate']], on='token1_index', how='left')
    
    # Set MUSD rate to 1.0
    df_result.loc[df_result['token0'] == 'MUSD', 'token0_usd_rate'] = 1.0
    df_result.loc[df_result['token1'] == 'MUSD', 'token1_usd_rate'] = 1.0
    df_result.loc[df_result['token0'] == 'upMUSD', 'token0_usd_rate'] = 1.0
    df_result.loc[df_result['token1'] == 'upMUSD', 'token1_usd_rate'] = 1.0
    
    # Convert amount columns to USD
    for col_base in ['totalVolume', 'amount']:
        # Handle regular columns (totalVolume0, amount0)
        if f'{col_base}0' in df_result.columns:
            df_result[f'{col_base}0'] = pd.to_numeric(df_result[f'{col_base}0'], errors='coerce').fillna(0)
            df_result[f'{col_base}0_usd'] = df_result[f'{col_base}0'] * df_result['token0_usd_rate']
        if f'{col_base}1' in df_result.columns:
            df_result[f'{col_base}1'] = pd.to_numeric(df_result[f'{col_base}1'], errors='coerce').fillna(0)
            df_result[f'{col_base}1_usd'] = df_result[f'{col_base}1'] * df_result['token1_usd_rate']
        
        # Handle swap columns (amount0In, amount0Out, amount1In, amount1Out)
        for suffix in ['In', 'Out']:
            if f'{col_base}0{suffix}' in df_result.columns:
                df_result[f'{col_base}0{suffix}'] = pd.to_numeric(df_result[f'{col_base}0{suffix}'], errors='coerce').fillna(0)
                df_result[f'{col_base}0{suffix}_usd'] = df_result[f'{col_base}0{suffix}'] * df_result['token0_usd_rate']
            if f'{col_base}1{suffix}' in df_result.columns:
                df_result[f'{col_base}1{suffix}'] = pd.to_numeric(df_result[f'{col_base}1{suffix}'], errors='coerce').fillna(0)
                df_result[f'{col_base}1{suffix}_usd'] = df_result[f'{col_base}1{suffix}'] * df_result['token1_usd_rate']
    
    return df_result

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
    df_with_usd.loc[df_with_usd[token_column] == 'upMUSD', 'usd'] = 1.0
    
    # Auto-detect amount columns if not provided
    if amount_columns is None:
        amount_columns = [col for col in df.columns if 'amount' in col.lower() and col != 'amount_usd']
    
    # Add USD conversion for each amount column
    for col in amount_columns:
        if col in df_with_usd.columns:
            usd_col_name = f"{col}_usd" if not col.endswith('_usd') else col
            df_with_usd[usd_col_name] = df_with_usd[col] * df_with_usd['usd']
    
    return df_with_usd