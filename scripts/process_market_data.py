from dotenv import load_dotenv
import pandas as pd

from mezo.clients import BigQueryClient, SubgraphClient, SupabaseClient
from mezo.currency_config import MUSD_MARKET_MAP
from mezo.currency_utils import Conversions
from mezo.datetime_utils import format_datetimes
from mezo.queries import MUSDQueries
from mezo.visual_utils import ProgressIndicators, with_progress


@with_progress("Replacing product IDs with human-readable names")
def replace_market_items(df, col, musd_market_map):
    """
    Replaces values in the specified column using the musd_market_map dictionary.

    Parameters:
        df: The DataFrame containing the column to replace
        col: The column name to replace values in
        musd_market_map (dict): Dictionary mapping addresses/IDs to human-readable labels

    Returns:
        pd.DataFrame: Updated DataFrame with replaced values.
    """
    # normalizes the column to lowercase for matching
    df[col] = df[col].str.lower()

    # normalizes the map keys to lowercase
    normalized_map = {k.lower(): v for k, v in musd_market_map.items()}

    df[col] = df[col].map(normalized_map)

    return df


@with_progress("Processing donations data")
def process_donations_data(donations):
    """Process and format donations data.

    Note: Creates an internal copy to avoid mutating the input DataFrame.
    """
    conversions = Conversions()

    # Create explicit copy to avoid mutating the original DataFrame
    donations_formatted = donations.copy()

    # Replace recipient addresses with human-readable names
    donations_formatted = replace_market_items(donations_formatted, "recipient", MUSD_MARKET_MAP)

    # Format dates and currency amounts
    format_datetimes(donations_formatted, ["timestamp_"])
    donations_formatted = conversions.format_token_decimals(
        donations_formatted, amount_cols=["amount"]
    )

    # Rename columns for consistency
    donations_col_map = {"timestamp_": "date", "recipient": "item", "donor": "wallet"}
    donations_formatted = donations_formatted.rename(columns=donations_col_map)

    return donations_formatted


@with_progress("Processing purchases data")
def process_purchases_data(purchases):
    """Process and format purchases data.

    Note: Creates an internal copy to avoid mutating the input DataFrame.
    """
    conversions = Conversions()

    # Create explicit copy to avoid mutating the original DataFrame
    purchases_formatted = purchases.copy()

    # replaces product IDs with human-readable names
    purchases_formatted = replace_market_items(purchases_formatted, "productId", MUSD_MARKET_MAP)

    # formats dates and currency amounts
    format_datetimes(purchases_formatted, ["timestamp_"])
    purchases_formatted = conversions.format_token_decimals(
        purchases_formatted, amount_cols=["price"]
    )

    # renames columns for consistency
    purchases_col_map = {
        "timestamp_": "date",
        "productId": "item",
        "price": "amount",
        "customer": "wallet",
    }
    purchases_formatted = purchases_formatted.rename(columns=purchases_col_map)

    return purchases_formatted


@with_progress("Merging and cleaning market transactions")
def create_market_transactions(donations_formatted, purchases_formatted):
    """Merge donations and purchases data into unified market transactions"""
    # Merge the two datasets
    market_transactions = pd.merge(donations_formatted, purchases_formatted, how="outer").fillna(0)

    # Select and clean final columns (ID will be added by BigQuery update_table method)
    market_transactions_final = market_transactions[
        ["date", "item", "amount", "wallet", "transactionHash_"]
    ].copy()

    # Convert data types for Supabase compatibility
    market_transactions_final[["date", "item", "wallet"]] = market_transactions_final[
        ["date", "item", "wallet"]
    ].astype(str)
    market_transactions_final["amount"] = market_transactions_final["amount"].astype(int)

    market_transactions_final["count"] = 1

    return market_transactions_final


@with_progress("Fetching redemption codes from Supabase")
def fetch_redemption_codes(supabase):
    redemption_codes = supabase.fetch_table_data("store_redemption_codes")
    return redemption_codes


@with_progress("Fetching market transactions data")
def get_all_market_txns(subgraph_url, query, query_key):
    df = SubgraphClient.get_subgraph_data(subgraph_url, query, query_key)

    return df


@with_progress("Uploading data to BigQuery")
def upload_to_bigquery(bq, df, dataset_name, table_name, id_column):
    if df is not None and len(df) > 0:
        bq.update_table(df, dataset_name, table_name, id_column)
        ProgressIndicators.print_step(f"Uploaded {dataset_name} to BigQuery", "success")
    return df


def main(test_mode=False, skip_bigquery=False):
    """Main function to process market transaction data."""
    ProgressIndicators.print_header("MARKET DATA PROCESSING PIPELINE")

    if test_mode:
        print(f"\n{'🧪 TEST MODE ENABLED 🧪':^60}")
        if skip_bigquery:
            print(f"{'Skipping BigQuery uploads':^60}")
        print(f"{'─' * 60}\n")

    try:
        # ==========================================================
        # LOAD ENVIRONMENT VARIABLES
        # ==========================================================

        ProgressIndicators.print_step("Loading environment variables", "start")

        load_dotenv(dotenv_path="../.env", override=True)
        pd.options.display.float_format = "{:.5f}".format

        supabase = SupabaseClient(url="SUPABASE_URL_PROD", key="SUPABASE_KEY_PROD")

        if not skip_bigquery:
            bq = BigQueryClient(key="GOOGLE_CLOUD_KEY", project_id="mezo-portal-data")

        ProgressIndicators.print_step("Environment loaded successfully", "success")

        # ==========================================================
        # FETCH RAW DATA
        # ==========================================================

        # fetches the store_redemption_codes table from supabase
        redemption_codes = fetch_redemption_codes(supabase)

        # fetches donations and purchases from market-mezo subgraph (1.0.0)
        donations = get_all_market_txns(
            SubgraphClient.MUSD_MARKET_SUBGRAPH, MUSDQueries.GET_MARKET_DONATIONS, "donateds"
        )

        purchases = get_all_market_txns(
            SubgraphClient.MUSD_MARKET_SUBGRAPH, MUSDQueries.GET_MARKET_PURCHASES, "orderPlaceds"
        )

        ProgressIndicators.print_step(
            f"Loaded {len(donations)} donations and {len(purchases)} purchases", "success"
        )

        # ==========================================================
        # CLEAN DATA
        # ==========================================================

        # Processing functions now create internal copies, so raw DataFrames
        # (donations, purchases) remain unmodified for BigQuery raw uploads
        donations_processed = process_donations_data(donations)
        purchases_processed = process_purchases_data(purchases)

        # joins donations and purchases into one dataframe
        market_transactions_final = create_market_transactions(
            donations_processed, purchases_processed
        )

        # ==========================================================
        # UPLOAD ALL DATA TO BIGQUERY
        # ==========================================================

        if not skip_bigquery:
            datasets_to_upload = [
                (donations, "raw_data", "market_donations_raw", "transactionHash_"),
                (purchases, "raw_data", "market_purchases_raw", "transactionHash_"),
                (redemption_codes, "supabase", "dim_market_redemption_codes", "code"),
                (
                    market_transactions_final,
                    "staging",
                    "market_transactions_clean",
                    "transactionHash_",
                ),
            ]

            for df, dataset_name, table_name, id_column in datasets_to_upload:
                upload_to_bigquery(bq, df, dataset_name, table_name, id_column)

        # ==========================================================
        # DISPLAY SUMMARY STATISTICS
        # ==========================================================

        ProgressIndicators.print_summary_box(
            f"{ProgressIndicators.COIN} MARKET TRANSACTION SUMMARY {ProgressIndicators.COIN}",
            {
                "Unique items": ", ".join(
                    str(x) for x in market_transactions_final["item"].unique()
                ),
                "Total Transactions": len(market_transactions_final),
                "Total Amount": f"${market_transactions_final['amount'].sum(): ,.2f}",
                "Unique Items": market_transactions_final["item"].nunique(),
                "Unique Wallets": market_transactions_final["wallet"].nunique(),
                "Date Range": f"{market_transactions_final['date'].min()} to {market_transactions_final['date'].max()}",
            },
        )

    except Exception as e:
        ProgressIndicators.print_step(f"Critical error in main processing: {str(e)}", "error")
        ProgressIndicators.print_header(f"{ProgressIndicators.ERROR} PROCESSING FAILED")
        print(f"\n{ProgressIndicators.INFO} Error traceback:")
        print(f"{'─' * 50}")
        import traceback

        traceback.print_exc()
        print(f"{'─' * 50}")
        raise


if __name__ == "__main__":
    results = main()
