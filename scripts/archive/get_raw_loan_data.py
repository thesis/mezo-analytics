import pandas as pd
from mezo.clients import SubgraphClient
from mezo.queries import MUSDQueries
from mezo.data_utils import save_raw_data

pd.options.display.float_format = '{:.6f}'.format

def get_all_loans():
    """Calls the subgraph API endpoint for BorrowerOperations.sol and retrieves all MUSD loan activity data,
    including open trove, close trove, adjust trove, and refinance trove"""
    musd = SubgraphClient(url=SubgraphClient.BORROWER_OPS_SUBGRAPH, headers= SubgraphClient.SUBGRAPH_HEADERS)

    loans =  musd.fetch_subgraph_data(MUSDQueries.GET_LOANS, 'troveUpdateds')
    df = pd.DataFrame(loans)
    
    save_raw_data(df, 'musd_loans.csv')

    return df