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