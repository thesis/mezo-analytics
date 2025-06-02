import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries, MUSDQueries
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

def get_all_bridge_transactions():
    """Calls the subgraph API endpiont for 0xab13b8eecf5aa2460841d75da5d5d861fd5b8a39 (Mezo Portal contract)
    and retrieves all bridge transactions (currently only one-way into Mezo)"""
    portal = SubgraphClient(url=SubgraphClient.MEZO_PORTAL_SUBGRAPH, headers= SubgraphClient.SUBGRAPH_HEADERS)

    bridge = portal.fetch_subgraph_data(BridgeQueries.GET_BRIDGE_TRANSACTIONS, 'depositBridgeds')
    df = pd.DataFrame(bridge)
    
    save_raw_data(df, 'bridge_txns.csv')

    return df


def get_all_loans():
    """Calls the subgraph API endpoint for BorrowerOperations.sol and retrieves all MUSD loan activity data,
    including open trove, close trove, adjust trove, and refinance trove"""
    musd = SubgraphClient(url=SubgraphClient.BORROWER_OPS_SUBGRAPH, headers= SubgraphClient.SUBGRAPH_HEADERS)

    loans =  musd.fetch_subgraph_data(MUSDQueries.GET_LOANS, 'troveUpdateds')
    df = pd.DataFrame(loans)
    
    save_raw_data(df, 'musd_loans.csv')

    return df