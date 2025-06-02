import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries
from mezo.data_utils import save_raw_data

pd.options.display.float_format = '{:.6f}'.format

def get_all_autobridge_transactions():
    """Calls the subgraph API endpoint for 0xab13b8eecf5aa2460841d75da5d5d861fd5b8a39 (Mezo Portal contract)
    and retrieves transaction data for all deposits auto-bridged from matsnet to mainnet"""
    portal = SubgraphClient(url=SubgraphClient.MEZO_PORTAL_SUBGRAPH, headers= SubgraphClient.SUBGRAPH_HEADERS)

    autobridges =  portal.fetch_subgraph_data(BridgeQueries.GET_AUTOBRIDGE_TRANSACTIONS, 'depositAutoBridgeds')
    df = pd.DataFrame(autobridges)

    save_raw_data(df, 'autobridges.csv')

    return df