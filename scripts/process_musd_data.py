from dotenv import load_dotenv
import pandas as pd
import os
import requests
from mezo.currency_utils import format_musd_currency_columns, get_token_price
from mezo.datetime_utils import format_datetimes
from mezo.data_utils import add_rolling_values, add_pct_change_columns, add_cumulative_columns
from mezo.clients import BigQueryClient, SubgraphClient
from mezo.queries import MUSDQueries
from mezo.visual_utils import ProgressIndicators, ExceptionHandler, with_progress

################################################
# Define helper functions
################################################

@with_progress("Cleaning loan data")
def clean_loan_data(raw, sort_col, date_cols, currency_cols):
    """Clean and format loan data."""
    if not ExceptionHandler.validate_dataframe(raw, "Raw loan data", [sort_col]):
        raise ValueError("Invalid input data for cleaning")
    
    df = raw.copy().sort_values(by=sort_col, ascending=False)
    df = format_datetimes(df, date_cols)
    df = format_musd_currency_columns(df, currency_cols)
    df['count'] = 1
    return df

@with_progress("Calculating collateralization ratios")
def find_coll_ratio(df, token_id):
    """Computes the collateralization ratio"""
    usd = get_token_price(token_id)
    df['coll_usd'] = df['coll'] * usd
    df['coll_ratio'] = (df['coll_usd']/df['principal']).fillna(0)
    return df

@with_progress("Filtering loan subsets")
def get_loans_subset(df, operation: int, equals):
    """Create a df with only new, adjusted, or closed loans
    0 = opened, 1 = closed, 2 = adjusted, 3 = refinanced
    note: operation = 2 also includes liquidated loans, so we have to remove those manually
    """
    df_copy = df.copy()
    df_copy['operation'] = df_copy['operation'].astype(int)
    if equals is True:
        result = df_copy.loc[df_copy['operation'] == operation]
    elif equals is False:
        result = df_copy.loc[df_copy['operation'] != operation]
    return result

@with_progress("Processing liquidation data")
def process_liquidation_data(liquidations, troves_liquidated):
    """Merge raw liquidation data from two queries"""
    liquidation_df_merged = pd.merge(
        liquidations, 
        troves_liquidated, 
        how='left', 
        on='transactionHash_'
    )

    liquidation_df_merged = liquidation_df_merged[
        ['timestamp__x', 
        'liquidatedPrincipal', 
        'liquidatedInterest', 
        'liquidatedColl', 
        'borrower',
        'transactionHash_',
        'count_x'
        ]
    ]

    liquidations_df_final = liquidation_df_merged.rename(
        columns = {
            'timestamp__x': 'timestamp_', 
            'liquidatedPrincipal': 'principal', 
            'liquidatedInterest': 'interest',
            'liquidatedColl': 'coll',
            'count_x': 'count'
        }
    )

    liquidations_final = liquidations_df_final.copy()
    liquidations_final['coll'] = liquidations_final['coll'].astype(float)

    return pd.DataFrame(liquidations_final)

@with_progress("Creating daily loan aggregations")
def create_daily_loan_data(new_loans, closed_loans, adjusted_loans, latest_loans):
    """Create daily aggregated loan data"""
    daily_new_loans = new_loans.groupby(['timestamp_']).agg(
        loans_opened = ('count', 'sum'),
        borrowers = ('borrower', lambda x: x.nunique()),
        principal = ('principal', 'sum'),
        collateral = ('coll', 'sum'),
        interest = ('interest', 'sum')
    ).reset_index()

    daily_closed_loans = closed_loans.groupby(['timestamp_']).agg(
        loans_closed = ('count', 'sum'),
        borrowers_who_closed = ('borrower', lambda x: x.nunique())
    ).reset_index()

    daily_new_and_closed_loans = pd.merge(daily_new_loans, daily_closed_loans, how='outer', on='timestamp_').fillna(0)
    daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']] = daily_new_and_closed_loans[['loans_opened', 'borrowers', 'loans_closed', 'borrowers_who_closed']].astype('int')      
    
    daily_adjusted_loans = adjusted_loans.groupby(['timestamp_']).agg(
        loans_adjusted = ('count', 'sum'),
        borrowers_who_adjusted = ('borrower', lambda x: x.nunique())
    ).reset_index()

    daily_loan_data = pd.merge(daily_new_and_closed_loans, daily_adjusted_loans, how='outer', on='timestamp_').fillna(0)
    daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']] = daily_loan_data[['loans_adjusted', 'borrowers_who_adjusted']].astype(int)

    daily_balances = latest_loans.groupby(['timestamp_']).agg(
        musd = ('principal', 'sum'),
        interest = ('interest', 'sum'),
        collateral = ('coll', 'sum')
    ).reset_index()

    daily_balances = daily_balances.rename(
        columns={'musd': 'net_musd', 
                 'interest': 'net_interest',
                 'collateral': 'net_coll'}
    )

    daily_loans_merged = pd.merge(daily_loan_data, daily_balances, how='outer', on='timestamp_')

    cols = {
        'timestamp_': 'date', 
        'principal': 'gross_musd', 
        'collateral': 'gross_coll', 
        'interest': 'gross_interest',
        'borrowers_who_closed': 'closers', 
        'borrowers_who_adjusted': 'adjusters'
    }

    daily_loans_merged = daily_loans_merged.rename(columns=cols)

    daily_musd_final = add_rolling_values(daily_loans_merged, 30, ['net_musd', 'net_interest', 'net_coll']).fillna(0)
    daily_musd_final_2 = add_cumulative_columns(daily_musd_final, ['net_musd', 'net_interest', 'net_coll'])
    daily_musd_final_3 = add_pct_change_columns(daily_musd_final_2, ['net_musd', 'net_interest', 'net_coll'], 'daily').fillna(0)
    final_daily_musd = daily_musd_final_3.replace([float('inf'), -float('inf')], 0)
    final_daily_musd['date'] = pd.to_datetime(final_daily_musd['date']).dt.strftime('%Y-%m-%d')
    
    return final_daily_musd

@with_progress("Processing loan adjustments")
def process_loan_adjustments(adjusted_loans):
    """Break down adjusted loan types for analysis"""
    adjusted_loans = adjusted_loans.sort_values(by=['borrower', 'timestamp_'])
    first_tx = adjusted_loans.groupby('borrower').first().reset_index()

    adjusted_loans_merged = adjusted_loans.merge(
        first_tx[['borrower', 'principal', 'coll']], 
        on='borrower', 
        suffixes=('', '_initial')
    )

    # Loan increases
    increased_loans = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                            > adjusted_loans_merged['principal_initial']].copy()
    increased_loans['type'] = 1

    # Collateral changes
    coll_increased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                           > adjusted_loans_merged['coll_initial']].copy()
    coll_increased['type'] = 2

    coll_decreased = adjusted_loans_merged[adjusted_loans_merged['coll'] 
                                           < adjusted_loans_merged['coll_initial']].copy()
    coll_decreased['type'] = 3

    # MUSD Repayments
    principal_decreased = adjusted_loans_merged[adjusted_loans_merged['principal'] 
                                                < adjusted_loans_merged['principal_initial']].copy()
    principal_decreased['type'] = 4

    # Create final_adjusted_loans dataframe with type column
    final_adjusted_loans = pd.concat([
        increased_loans,
        coll_increased, 
        coll_decreased,
        principal_decreased
    ], ignore_index=True)
    
    return final_adjusted_loans

@with_progress("Fetching MUSD token data")
def fetch_musd_token_data():
    """Fetch MUSD token transfers and metadata"""
    def fetch_data(endpoint: str) -> pd.DataFrame:
        """Fetch data from the specified API endpoint."""
        base_url = 'http://api.explorer.mezo.org/api/v2/'
        url = f"{base_url}/{endpoint}"
        all_data = []
        next_page_params = None
        timeout = 10
        
        while True:
            response = requests.get(url, params=next_page_params or {}, timeout=timeout)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data: {response.status_code}")

            data = response.json()
            items = data.get('items', [])
            if not items:
                break

            all_data.append(pd.json_normalize(items))
            next_page_params = data.get("next_page_params")
            if not next_page_params:
                break

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    musd_token_address = '0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186'
    # musd_transfers = fetch_data(f'tokens/{musd_token_address}/transfers')

    # Get holder data
    base_url = 'http://api.explorer.mezo.org/api/v2/tokens/'
    url = f'{base_url}{musd_token_address}/counters'
    response = requests.get(url, timeout=10)
    data = response.json()
    dat = pd.json_normalize(data)
    musd_holders = pd.DataFrame(dat)

    # Get token data
    url2 = f'{base_url}{musd_token_address}/'
    response = requests.get(url2, timeout=10)
    data2 = response.json()
    dat2 = pd.json_normalize(data2)
    musd_token_data = pd.DataFrame(dat2)

    musd_token = pd.merge(musd_token_data, musd_holders, how='cross')
    musd_token = musd_token[['circulating_market_cap', 'exchange_rate', 
                           'holders', 'total_supply', 'volume_24h', 
                           'token_holders_count', 'transfers_count']]

    format_musd_currency_columns(musd_token, ['total_supply'])
    musd_token = musd_token.fillna(0)
    
    return musd_token

# @with_progress("Fetching redemptions data")
# def get_redemptions():
#     """
#     Get redemption data for troves from the MUSD Trove Manager subgraph
#     """
#     musd = SubgraphClient(
#         url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
#         headers=SubgraphClient.SUBGRAPH_HEADERS
#     )
    
#     print("🔍 Trying redemptions query...")
#     try:
#         redemptions_data = musd.fetch_subgraph_data(
#             MUSDQueries.GET_REDEMPTIONS, 
#             'redemptions'
#         )
    
#         if redemptions_data:
#             redemptions_df = pd.DataFrame(redemptions_data)
#             print(f"✅ Found {len(redemptions_df)} redemption records")
            
#             return redemptions_df
#         else:
#             print("⚠️ redemptions query returned no data")
#     except Exception as e:
#         print(f"❌ redemptions query failed: {e}")

def main():
    """Main function to process MUSD loan data."""
    ProgressIndicators.print_header("MUSD DATA PROCESSING PIPELINE")

    try:
        ################################################
        # Setup env and clients
        ################################################
        # Load environment variables
        ProgressIndicators.print_step("Loading environment variables", "start")
        load_dotenv(dotenv_path='../.env', override=True)
        COINGECKO_KEY = os.getenv('COINGECKO_KEY')
        ProgressIndicators.print_step("Environment loaded successfully", "success")

        # Initialize clients
        ProgressIndicators.print_step("Initializing BigQuery", "start")
        bq = BigQueryClient(key='GOOGLE_CLOUD_KEY', project_id='mezo-portal-data')
        ProgressIndicators.print_step("BigQuery initialized", "success")

        ################################################
        # Get raw data from subgraphs
        ################################################

        ProgressIndicators.print_step("Fetching raw loan data from subgraphs", "start")
        raw_loans = SubgraphClient.get_subgraph_data(
            SubgraphClient.BORROWER_OPS_SUBGRAPH,
            MUSDQueries.GET_LOANS,
            'troveUpdateds'
        )

        raw_liquidations = SubgraphClient.get_subgraph_data(
            SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
            MUSDQueries.GET_MUSD_LIQUIDATIONS,
            'liquidations'
        )

        raw_troves_liquidated = SubgraphClient.get_subgraph_data(
            SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH,
            MUSDQueries.GET_LIQUIDATED_TROVES,
            'troveLiquidateds'
        )

        raw_redemptions = SubgraphClient.get_subgraph_data(
            SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
            MUSDQueries.GET_REDEMPTIONS, 
            'redemptions'
        )

        raw_fees = SubgraphClient.get_subgraph_data(
            SubgraphClient.BORROWER_OPS_SUBGRAPH, 
            MUSDQueries.GET_BORROW_FEES, 
            'borrowingFeePaids'
        )
        ProgressIndicators.print_step("Raw data fetched successfully", "success")

        ################################################
        # Upload raw data to BigQuery
        ################################################
        
        ProgressIndicators.print_step("Uploading raw data to BigQuery", "start")
        raw_datasets_to_upload = [
            (raw_loans, 'musd_loans_raw', 'transactionHash_'),
            (raw_liquidations, 'musd_liquidations_raw', 'transactionHash_'),
            (raw_troves_liquidated, 'musd_troves_liquidated_raw', 'transactionHash_'),
            (raw_redemptions, 'musd_redemptions_raw', 'transactionHash_'),
            (raw_fees, 'musd_fees_raw', 'transactionHash_')
        ]

        for dataset, table_name, id_column in raw_datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                dataset['transactionHash_'] = dataset['transactionHash_'].astype(str)
                bq.update_table(dataset, 'raw_data', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        # if raw_loans is not None and len(raw_loans) > 0:
        #     bq.update_table(raw_loans, 'raw_data', 'musd_loans_raw', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded raw_loans to BigQuery", "success")

        # if raw_liquidations is not None and len(raw_liquidations) > 0:
        #     bq.update_table(raw_liquidations, 'raw_data', 'musd_liquidations_raw', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded raw_liquidations to BigQuery", "success")

        # if raw_troves_liquidated is not None and len(raw_troves_liquidated) > 0:
        #     bq.update_table(raw_troves_liquidated, 'raw_data', 'musd_troves_liquidated_raw', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded raw_troves_liquidated to BigQuery", "success")

        # if raw_redemptions is not None and len(raw_redemptions) > 0:
        #     bq.update_table(raw_redemptions, 'raw_data', 'musd_redemptions_raw', 'transactionHash_')
        #     ProgressIndicators.print_step("Uploaded raw_redemptions to BigQuery", "success")

        ################################################
        # Clean and process loan data
        ################################################
        
        # all loans
        loans = clean_loan_data(
            raw_loans,
            sort_col='timestamp_', 
            date_cols=['timestamp_'], 
            currency_cols=['principal', 'coll', 'stake', 'interest']
        )

        loans = find_coll_ratio(loans, 'bitcoin')

        # liquidated loans
        liquidations = clean_loan_data(
            raw_liquidations,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['liquidatedPrincipal', 'liquidatedInterest', 'liquidatedColl']
        )

        troves_liquidated = clean_loan_data(
            raw_troves_liquidated,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['debt', 'coll']
        )

        liquidations_final = process_liquidation_data(liquidations, troves_liquidated)

        # redemptions
        redemptions = clean_loan_data(
            raw_redemptions,
            sort_col='timestamp_',
            date_cols=['timestamp_'],
            currency_cols=['actualAmount', 'attemptedAmount', 'collateralFee', 'collateralSent']
        )
        
        ################################################
        # Create subsets of loan df by type of txn
        ################################################

        # Create loan subsets
        new_loans = get_loans_subset(loans, 0, True)
        closed_loans = get_loans_subset(loans, 1, True)
        adjusted_loans = get_loans_subset(loans, 2, True)
        refinanced_loans = get_loans_subset(loans, 3, True)

        # Remove liquidations from adjusted loans
        liquidated_borrowers = liquidations_final['borrower'].unique()
        adjusted_loans = adjusted_loans[~adjusted_loans['borrower'].isin(liquidated_borrowers)]

        # Get latest loans
        latest_loans = loans.drop_duplicates(subset='borrower', keep='first')
        latest_open_loans = get_loans_subset(latest_loans, 1, False)
        latest_open_loans = latest_open_loans[~latest_open_loans['borrower'].isin(liquidated_borrowers)]

        # Process loan adjustments
        final_adjusted_loans = process_loan_adjustments(adjusted_loans)

        ################################################
        # Upload clean and subset dfs to BigQuery
        ################################################

        ProgressIndicators.print_step("Uploading loan subset data to BigQuery", "start")

        datasets_to_upload = [
            (loans, 'all_loans_clean', 'transactionHash_'),
            (new_loans, 'new_loans_clean', 'transactionHash_'),
            (closed_loans, 'closed_loans_clean', 'transactionHash_'),
            (latest_open_loans, 'open_loans_clean', 'transactionHash_'),
            (final_adjusted_loans, 'adjusted_loans_clean', 'transactionHash_'),
            (liquidations_final, 'liquidated_loans_clean', 'transactionHash_'),
            (refinanced_loans, 'refinanced_loans_clean', 'transactionHash_'),
            (redemptions, 'redemptions_clean', 'transactionHash_')
        ]

        for dataset, table_name, id_column in datasets_to_upload:
            if dataset is not None and len(dataset) > 0:
                bq.update_table(dataset, 'staging', table_name, id_column)
                ProgressIndicators.print_step(f"Uploaded {table_name} to BigQuery", "success")

        ################################ 
        # Create daily aggregations
        ################################

        final_daily_musd = create_daily_loan_data(new_loans, closed_loans, adjusted_loans, latest_loans)

        ################################################
        # Upload daily loan data to BigQuery
        ################################################
        
        ProgressIndicators.print_step("Uploading daily loans data to BigQuery", "start")
        if final_daily_musd is not None and len(final_daily_musd) > 0:
            bq.update_table(final_daily_musd, 'staging', 'daily_loans_clean', 'date')
            ProgressIndicators.print_step("Uploaded final_daily_musd to BigQuery", "success")

        # Calculate summary statistics
        ProgressIndicators.print_step("Calculating summary statistics", "start")
        
        # All-time stats
        btc_price = get_token_price('bitcoin')
        musd_token = fetch_musd_token_data()

        all_time_musd_borrowed = new_loans['principal'].sum()
        all_time_musd_loans = new_loans['count'].sum()
        all_time_musd_borrowers = new_loans['borrower'].nunique()
        all_time_closed_loans = closed_loans['count'].sum()
        all_time_adjustments = adjusted_loans['count'].sum()

        # Current system health
        open_loans = latest_open_loans['count'].sum()
        liquidated_loans = liquidations_final['count'].sum()
        interest_liquidated = liquidations_final['interest'].sum()
        coll_liquidated = liquidations_final['coll'].sum()

        system_coll = latest_open_loans['coll'].sum()
        system_debt = latest_open_loans['principal'].sum() + latest_open_loans['interest'].sum()
        TCR = ((system_coll * btc_price) / system_debt) * 100

        # Create summary dataframes
        musd_summary = pd.DataFrame([{
            'all_time_musd_borrowed': all_time_musd_borrowed, 
            'all_time_loans': all_time_musd_loans,
            'all_time_borrowers': all_time_musd_borrowers, 
            'system_debt': system_debt, 
            'system_coll': system_coll,
            'TCR': TCR, 
            'open_loans': open_loans,
            'closed_loans': all_time_closed_loans,
            'liquidated_loans': liquidated_loans,
            'interest_liquidated': interest_liquidated,
            'collateral_liquidated': coll_liquidated
        }])
        ProgressIndicators.print_step("Summary statistics calculated", "success")

        ProgressIndicators.print_summary_box(
            f"💰 MUSD LOAN SUMMARY STATISTICS 💰",
            {
                "Total MUSD Borrowed": all_time_musd_borrowed,
                "Total Loans": all_time_musd_loans,
                "Unique Borrowers": all_time_musd_borrowers,
                "Open Loans": open_loans,
                "System TCR": f"{TCR:.2f}%",
                "System Debt": system_debt
            }
        )

        ProgressIndicators.print_header(f"🚀 MUSD PROCESSING COMPLETED SUCCESSFULLY 🚀")
        
        return {
            'daily_musd_data': final_daily_musd,
            'summary': musd_summary,
            'token_data': musd_token
        }
        
    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"❌ PROCESSING FAILED")
        print(f"\n📍 Error traceback:")
        print(f"{'─' * 50}")
        import traceback
        traceback.print_exc()
        print(f"{'─' * 50}")
        raise

if __name__ == "__main__":
    results = main()