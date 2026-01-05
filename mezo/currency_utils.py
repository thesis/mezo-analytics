from decimal import Decimal
import os

import pandas as pd
import requests

from mezo.currency_config import (
    MEZO_ASSET_NAMES_MAP,
    POOL_TOKEN_PAIRS,
    TOKEN_MAP,
    TOKENS_ID,
    TOKENS_ID_MAP,
)


class Conversions:

    def __init__(self):
        self.coingecko_key = os.getenv('COINGECKO_KEY')
        self.DEFAULT_DECIMALS = 1e18
        self.DECIMALS_MAP = {
            'USDC': 1e6,
            'USDT': 1e6,
            'mUSDC': 1e6,
            'mUSDT': 1e6,
            
            'WBTC': 1e8,
            'FBTC': 1e8,
            'cbBTC': 1e8,
            'swBTC': 1e8,
        }
        self.MEZO_STABLES = ['MUSD', 'upMUSD']

    def _standardize_token_symbols(self, df, cols):
        """ 
        Removes the m prefix on Mezo assets for easier price conversions
        Uses the MEZO_ASSET_NAMES_MAP in currency_config

        Parameters:
            df: the dataframe containing columns with token symbols with with 'm' prefix
            cols: the columns in the dataframe with token symbols with the 'm' prefix
        """
        token_columns = [cols] if isinstance(cols, str) else cols

        for col in token_columns:
            df[col] = df[col].replace(MEZO_ASSET_NAMES_MAP)

        return df
    
    def _add_usd_rate_column(self, df: pd.DataFrame, token_column: str, rate_column_name: str = 'usd_rate') -> pd.DataFrame:
        """
        Add a USD rate column for a given token column

        Args:
            df: DataFrame containing token data
            token_column: Name of column containing token symbols
            rate_column_name: Name for the new USD rate column

        Returns:
            DataFrame with added rate column
        """
        prices = self.get_token_prices()
        if prices is None or prices.empty:
            raise ValueError("No token prices received from API")

        token_usd_prices = prices.T.reset_index()

        df_result = df.copy()

        # Standardize token symbols before mapping (e.g., 'mT' -> 'T')
        df_result = self._standardize_token_symbols(df_result, token_column)

        # Use .get() to handle tokens not in TOKENS_ID_MAP gracefully
        df_result['_temp_index'] = df_result[token_column].apply(
            lambda x: TOKENS_ID_MAP.get(x) if pd.notna(x) else None
        )

        df_result = pd.merge(df_result, token_usd_prices, left_on='_temp_index', right_on='index', how='left')
        df_result = df_result.rename(columns={'usd': rate_column_name})
        df_result = df_result.drop(columns=['_temp_index', 'index'])

        # Set Mezo stablecoins to 1.0
        df_result.loc[df_result[token_column].isin(self.MEZO_STABLES), rate_column_name] = 1.0

        return df_result
    
    def get_token_prices(self):
        """ Retrieves USD conversion price for all tokens from Coingecko """
        url = 'https://api.coingecko.com/api/v3/simple/price'
        params = {'ids': TOKENS_ID, 'vs_currencies': 'usd'}
        headers = {'x-cg-demo-api-key': self.coingecko_key}

        response = requests.get(url, params = params)

        data = response.json()
        df = pd.DataFrame(data)

        return df

    def get_token_price(self, token_id):
        """ Retrieves a single token's USD conversion price from Coingecko """
        url = 'https://api.coingecko.com/api/v3/simple/price'
        params = {'ids': token_id, 'vs_currencies': 'usd'}
        headers = {'x-cg-demo-api-key': self.coingecko_key}

        response = requests.get(url, params = params)

        data = response.json()
        price = data[token_id]['usd']
        
        return price

    def format_token_decimals(self, df, amount_cols, token_name_col=None):
        """
        Format token amount columns by removing decimals
        
        Parameters:
            df : pd.DataFrame
                DataFrame containing token amount data
            amount_cols : str or list
                Column name(s) to format
            token_name_col : str, optional
                Column containing token symbols. If provided, uses token-specific decimals.
                If None, uses DEFAULT_DECIMALS for all columns
        """
        if isinstance(amount_cols, str):
            amount_cols = [amount_cols]

        df[amount_cols] = df[amount_cols].apply(pd.to_numeric, errors='coerce')  # MOVED OUTSIDE IF/ELSE

        if token_name_col is not None:
            self._standardize_token_symbols(df, token_name_col)
            for col in amount_cols:
                df[col] = df.apply(
                    lambda row: row[col] / self.DECIMALS_MAP.get(row[token_name_col], self.DEFAULT_DECIMALS),
                    axis=1
                )
        else:
            df[amount_cols] = df[amount_cols] / self.DEFAULT_DECIMALS  # VECTORIZED
    
        return df

    def add_usd_conversions(self, df, token_column, amount_columns):
        """
        Add USD price conversions to token amount columns
        
        Args:
            df: DataFrame containing token data
            token_column: Name of column containing token identifiers
            amount_columns: List of amount columns to convert
        """
        df_result = self._add_usd_rate_column(df, token_column, 'usd_rate')
        
        for col in amount_columns:
            if col in df_result.columns:
                usd_col_name = f"{col}_usd" if not col.endswith('_usd') else col
                df_result[usd_col_name] = df_result[col] * df_result['usd_rate']
        
        return df_result
     
    def add_multi_token_usd_conversions(self, df, token_configs):
        """
        Add USD conversions for multiple token columns (e.g., liquidity pools with token0/token1)
        
        Args:
            df: DataFrame containing token data
            token_configs: List of dicts with format:
                [
                    {'token_col': 'token0', 'amount_cols': ['amount0', 'amount0In', 'amount0Out']},
                    {'token_col': 'token1', 'amount_cols': ['amount1', 'amount1In', 'amount1Out']}
                ]
        
        Returns:
            DataFrame with USD conversions for all specified token/amount column pairs
        """
        df_result = df.copy()
        
        # Add USD rate columns for each token
        for config in token_configs:
            token_col = config['token_col']
            rate_col = f"{token_col}_usd_rate"
            df_result = self._add_usd_rate_column(df_result, token_col, rate_col)
        
        # Convert amount columns to USD
        for config in token_configs:
            token_col = config['token_col']
            rate_col = f"{token_col}_usd_rate"
            
            for amount_col in config['amount_cols']:
                if amount_col in df_result.columns:
                    df_result[amount_col] = pd.to_numeric(df_result[amount_col], errors='coerce').fillna(0)
                    df_result[f"{amount_col}_usd"] = df_result[amount_col] * df_result[rate_col]
        
        return df_result

    def replace_token_addresses_with_symbols(self, df, token_column, token_map):
        """
        Replaces token addresses with token symbols using a token map dict in currency_config.
        Args:
            df: DataFrame containing token data
            token_column: Name of column containing token addresses
            token_map: Dict mapping token addresses to symbols. TOKEN_MAP for Ethereum, MEZO_TOKEN_ADDRESSES for Mezo
        """
        df[token_column] = df[token_column].str.lower()
        normalized_map = {k.lower(): v for k, v in token_map.items()}
        df[token_column] = df[token_column].replace(normalized_map)

        return df
    
    def map_pool_to_tokens(self, df, pool_column, pool_token_mapping):
        """
        Add token0 and token1 columns based on pool identifier mapping
        
        Args:
            df: DataFrame containing pool data
            pool_column: Name of column containing pool identifiers
            pool_token_mapping: Dictionary mapping pool IDs to token pairs
                Example: {'pool_id': {'token0': 'USDC', 'token1': 'WBTC'}}
        """

        df['token0'] = df[pool_column].map(lambda x: pool_token_mapping.get(x, {}).get('token0'))
        df['token1'] = df[pool_column].map(lambda x: pool_token_mapping.get(x, {}).get('token1'))
        
        return df