import pandas as pd
from dotenv import load_dotenv
import os
from mezo.currency_utils import format_token_columns
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pct_change_columns, add_cumulative_columns, flatten_json_column
from mezo.clients import BigQueryClient, SubgraphClient, Web3Client
from mezo.currency_config import POOL_TOKEN_PAIRS, TOKENS_ID_MAP, POOLS_MAP
from mezo.currency_utils import format_pool_token_columns, add_pool_usd_conversions
from mezo.currency_utils import get_token_prices, add_pool_usd_conversions, add_usd_conversions
from mezo.currency_config import TOKENS_ID_MAP, TIGRIS_MAP
from mezo.queries import PoolQueries

load_dotenv(dotenv_path='../.env', override=True)
COINGECKO_KEY = os.getenv('COINGECKO_KEY')

# Instantiate subgraph
pool_volumes = SubgraphClient(
    url = SubgraphClient.TIGRIS_POOLS_SUBGRAPH,
    headers = SubgraphClient.SUBGRAPH_HEADERS
)

# Fetch subgraph data
dat =  pool_volumes.fetch_subgraph_data(PoolQueries.GET_POOL_VOLUME, 'poolVolumes')

# Create df with data and flatten nested cols
df = pd.DataFrame(dat)
df_flat = flatten_json_column(df, 'pool')
df_flat = df_flat.drop(columns=['pool'])

# Replace token symbols with the ethereum ones

token_columns = ['pool_token0_symbol', 'pool_token1_symbol']
volume_columns = ['totalVolume0', 'totalVolume1']

for col in token_columns:
    df_flat[col] = df_flat[col].replace({
        'mUSDC': 'USDC',
        'mUSDT': 'USDT'
    })

df_flat['pool'] = df_flat['pool_name'].map(TIGRIS_MAP)

# Do some cleaning 🧹

def clean_volume_df(df):
    
    df_clean = df.copy()

    df_clean = format_datetimes(df_clean, ['timestamp'])
    df_clean = format_token_columns(
        df_clean, 
        ['totalVolume0'], 
        'pool_token0_symbol'
    )

    df_clean = format_token_columns(
        df_clean, 
        ['totalVolume1'],
        'pool_token1_symbol'
    )

    df_clean = add_usd_conversions(
        df_clean, 
        'pool_token0_symbol',
        TOKENS_ID_MAP, 
        ['totalVolume0']
    )

    # remove 'usd' and 'index' columns from previous df to avoid conflicts with converting the totalVolume1 column
    df_clean = df_clean[[
        'timestamp', 
        'totalVolume0', 
        'totalVolume1', 
        'pool_name',
        'pool_token0_symbol', 
        'pool_token1_symbol', 
        'pool', 
        'totalVolume0_usd'
    ]]

    # repeat totalVolume0 to usd with totalVolume1
    df_clean = add_usd_conversions(
        df_clean, 
        token_column='pool_token1_symbol', 
        tokens_id_map=TOKENS_ID_MAP, 
        amount_columns=['totalVolume1']
    )

    df_clean['count'] = 1 # count column is for tabulating total txns later on

    # trim columns
    volume_final = df_clean[[
        'timestamp', 
        'totalVolume0', 
        'totalVolume1', 
        'pool_name', 
        'pool',
        'totalVolume0_usd', 
        'totalVolume1_usd', 
        'count'
    ]]

    return volume_final

volume_final = clean_volume_df(df_flat)


############ do the rest of this in hex

# Create DFs for each pool for examining data
btc_musd_pool = volume_final.loc[volume_final['pool_name'] == 'Volatile AMM - BTC/MUSD']

# Current epoch
test = btc_musd_pool[0:6]
test['totalVolume0_usd'].sum() + test['totalVolume1_usd'].sum()
vol0_usd = (btc_musd_pool['totalVolume0_usd'].sum())/2 # says btc volume in usd is 237,615.47
vol0 = btc_musd_pool['totalVolume0'].sum() # says btc volume in btc is 2.0843
vol1 = (btc_musd_pool['totalVolume1_usd'].sum())/2 # says musd volume is 244,418.48

vol0_usd + vol1

musd_musdt_pool = volume_final.loc[volume_final['pool_name'] == 'Stable AMM - MUSD/mUSDT']
musdc_musd_pool = volume_final.loc[volume_final['pool_name'] == 'Stable AMM - mUSDC/MUSD']


volume_by_pool = volume_final.groupby(['pool_name']).agg(
        total_volume_0 = ('totalVolume0_usd', 'sum'),
        total_volume_1 = ('totalVolume1_usd', 'sum')
    ).reset_index()


####################################################################################

pool_deposits = SubgraphClient(
    url = 'https://api.goldsky.com/api/public/project_cm6ks2x8um4aj01uj8nwg1f6r/subgraphs/musd-pools-mezo/1.0.0/gn',
    headers = SubgraphClient.SUBGRAPH_HEADERS
)

# Fetch subgraph data
pool_deposit_data =  pool_deposits.fetch_subgraph_data(PoolQueries.GET_DEPOSITS, 'mints')

raw = pd.DataFrame(pool_deposit_data)
raw['contractId_'].unique()

def clean_deposits_data(df):
    ddf = df.copy()

    ddf['pool'] = ddf['contractId_'].map(POOLS_MAP)
    ddf = format_datetimes(ddf, ['timestamp_'])
    ddf = format_pool_token_columns(ddf, 'contractId_', POOL_TOKEN_PAIRS)
    ddf = add_pool_usd_conversions(ddf, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)

    return ddf

pool_deposits = clean_deposits_data(raw)

####################################################################################

# Instantiate subgraph
pool_withdrawals = SubgraphClient(
    url = SubgraphClient.POOLS_SUBGRAPH,
    headers = SubgraphClient.SUBGRAPH_HEADERS
)

# Fetch subgraph data
pool_wit_data =  pool_withdrawals.fetch_subgraph_data(PoolQueries.GET_WITHDRAWALS, 'burns')

withdrawals_df = pd.DataFrame(pool_wit_data)

def clean_withdrawal_data(df):
    wdf = df.copy()

    wdf['pool'] = wdf['contractId_'].map(POOLS_MAP)
    wdf = format_datetimes(wdf, ['timestamp_'])
    wdf = format_pool_token_columns(wdf, 'contractId_', POOL_TOKEN_PAIRS)
    wdf = add_pool_usd_conversions(wdf, 'contractId_', POOL_TOKEN_PAIRS, TOKENS_ID_MAP)

    return wdf

pool_withdrawals = clean_withdrawal_data(withdrawals_df)

####################################################################################

# Instantiate subgraph
fees_raw = SubgraphClient(
    url = SubgraphClient.TIGRIS_POOLS_SUBGRAPH,
    headers = SubgraphClient.SUBGRAPH_HEADERS
)

# Fetch subgraph data
fees_data =  fees_raw.fetch_subgraph_data(PoolQueries.GET_TOTAL_POOL_FEES, 'feesStats_collection')

# Save pool volume data df
fees_df = pd.DataFrame(fees_data)
fees_df.columns

# flatten col
fees_df_flat = flatten_json_column(fees_df, 'pool')
fees_df_flat = fees_df_flat.drop(columns=['pool'])

# Replace mUSDC and mUSDT with USDC and USDT
fees_df_flat['pool_token0_symbol'] = fees_df_flat['pool_token0_symbol'].replace({
    'mUSDC': 'USDC', 
    'mUSDT': 'USDT'
})

fees_df_flat['pool_token1_symbol'] = fees_df_flat['pool_token1_symbol'].replace({
    'mUSDC': 'USDC', 
    'mUSDT': 'USDT'
})

fees_df_flat['pool'] = fees_df_flat['pool_name'].map(TIGRIS_MAP)

fees_df_flat = format_datetimes(fees_df_flat, ['timestamp'])
fees_df_flat = format_token_columns(fees_df_flat, ['totalFees0'], 'pool_token0_symbol')
fees_df_flat = format_token_columns(fees_df_flat, ['totalFees1'], 'pool_token1_symbol')

# Add USD conversions for totalFees0
fees_df_flat = add_usd_conversions(fees_df_flat, 'pool_token0_symbol', TOKENS_ID_MAP, ['totalFees0'])

fees_df_flat.columns
fees_df_flat = fees_df_flat.drop(columns=['index', 'usd'])

# Add USD conversions for totalFees1 
fees_df_flat = add_usd_conversions(fees_df_flat, 'pool_token1_symbol', TOKENS_ID_MAP, ['totalFees1'])

# Add count column
fees_df_flat['count'] = 1
fees_df_final = fees_df_flat.drop(columns=['index', 'usd', 'pool_token0_symbol', 'pool_token1_symbol'])
fees_df_final.columns