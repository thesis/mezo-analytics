import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import BridgeQueries, MUSDQueries
# from mezo.data_utils import save_raw_data

pd.options.display.float_format = '{:.6f}'.format

def get_all_autobridge_transactions():
    """Calls the subgraph API endpoint for 0xab13b8eecf5aa2460841d75da5d5d861fd5b8a39 (Mezo Portal contract)
    and retrieves transaction data for all deposits auto-bridged from matsnet to mainnet"""
    portal = SubgraphClient(
        url=SubgraphClient.MEZO_PORTAL_SUBGRAPH, 
        headers= SubgraphClient.SUBGRAPH_HEADERS
    )

    autobridges =  portal.fetch_subgraph_data(
        BridgeQueries.GET_AUTOBRIDGE_TRANSACTIONS, 
        'depositAutoBridgeds'
    )
    
    df = pd.DataFrame(autobridges)
    # save_raw_data(df, 'autobridges.csv')

    return df

def get_all_bridge_transactions():
    """Calls the subgraph API endpiont for 0xab13b8eecf5aa2460841d75da5d5d861fd5b8a39 (Mezo Portal contract)
    and retrieves all bridge transactions (currently only one-way into Mezo)"""
    portal = SubgraphClient(
        url=SubgraphClient.MEZO_BRIDGE_SUBGRAPH, 
        headers= SubgraphClient.SUBGRAPH_HEADERS
    )

    bridge = portal.fetch_subgraph_data(
        BridgeQueries.GET_BRIDGE_TRANSACTIONS, 
        'assetsLockeds'
    )
    
    df = pd.DataFrame(bridge)
    # save_raw_data(df, 'bridge_txns.csv')

    return df


def get_all_loans():
    """Calls the subgraph API endpoint for BorrowerOperations.sol and retrieves all MUSD loan activity data,
    including open trove, close trove, adjust trove, and refinance trove"""
    musd = SubgraphClient(
        url=SubgraphClient.BORROWER_OPS_SUBGRAPH, 
        headers= SubgraphClient.SUBGRAPH_HEADERS
    )

    print("🔍 Trying troveUpdates query...")

    try:
        loans =  musd.fetch_subgraph_data(
            MUSDQueries.GET_LOANS, 
            'troveUpdateds'
        )
        if loans:
            df = pd.DataFrame(loans)
            print(f"✅ Found {len(loans)} loan records")
            
            # save_raw_data(df, 'musd_loans.csv')
            return df
        else:
            print("⚠️ troveUpdates query returned no data")
    except Exception as e:
        print(f"❌ troveUpdates query failed: {e}")

def get_liquidation_data():
    """
    Get liquidation data from the MUSD Trove Manager subgraph
    """
    musd = SubgraphClient(
        url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )
    
    print("🔍 Trying liquidations query...")
    try:
        liquidations_data =  musd.fetch_subgraph_data(
            MUSDQueries.GET_MUSD_LIQUIDATIONS, 
            'liquidations'
        )
        if liquidations_data:
            liquidations_df = pd.DataFrame(liquidations_data)
            print(f"✅ Found {len(liquidations_df)} liquidation records")
            
            # save_raw_data(liquidations_df, 'musd_liquidations.csv')
            return liquidations_df
        else:
            print("⚠️ liquidations query returned no data")
    except Exception as e:
        print(f"❌ liquidations query failed: {e}")

def get_trove_liquidated_data():
    """
    Get liquidation data for troves from the MUSD Trove Manager subgraph
    """
    musd = SubgraphClient(
        url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )
    
    print("🔍 Trying troveLiquidateds query...")
    try:
        trove_liquidated_data = musd.fetch_subgraph_data(
            MUSDQueries.GET_LIQUIDATED_TROVES, 
            'troveLiquidateds'
        )
    
        if trove_liquidated_data:
            trove_liquidated_df = pd.DataFrame(trove_liquidated_data)
            print(f"✅ Found {len(trove_liquidated_df)} trove liquidation records")
            
            # save_raw_data(trove_liquidated_df, 'musd_troves_liquidated.csv')
            return trove_liquidated_df
        else:
            print("⚠️ troveLiquidateds query returned no data")
    except Exception as e:
        print(f"❌ troveLiquidateds query failed: {e}")


def get_all_market_donations():
    """Calls the subgraph API endpoint for the MUSD market contracts to retrieve donations"""
    portal = SubgraphClient(
        url=SubgraphClient.MUSD_MARKET_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )

    donations = portal.fetch_subgraph_data(
        MUSDQueries.GET_MARKET_DONATIONS, 
        'donateds'
    )
    
    df = pd.DataFrame(donations)
    return df


def get_all_market_purchases():
    """Calls the subgraph API endpoint for the MUSD market contracts to retrieve purchases"""
    portal = SubgraphClient(
        url=SubgraphClient.MUSD_MARKET_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )

    purchases = portal.fetch_subgraph_data(
        MUSDQueries.GET_MARKET_PURCHASES, 
        'orderPlaceds'
    )
    
    df = pd.DataFrame(purchases)
    return df