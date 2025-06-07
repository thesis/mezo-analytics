# Liquidation Data Troubleshooting Script
import pandas as pd
import requests
import json
from mezo.clients import SubgraphClient

# First, let's create a debug version of the SubgraphClient method
def debug_fetch_subgraph_data(client, query, method):
    """Debug version that shows us exactly what's happening"""
    all_results = []
    skip = 0
    batch_size = 1000

    print(f"🔍 Starting to fetch liquidation data...")
    print(f"URL: {client.url}")
    print(f"Query method: {method}")
    
    while True:
        print(f"\n📡 Fetching batch with skip={skip}...")

        response = requests.post(
            url=client.url,
            headers=client.headers,
            json={"query": query, "variables": {"skip": skip}}
        )

        print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code} - {response.text}")
            break

        try:
            data = response.json()
            print(f"📦 Raw response keys: {list(data.keys())}")
            
            if 'data' in data:
                print(f"📊 Data keys: {list(data['data'].keys())}")
                transactions = data.get("data", {}).get(method, [])
                print(f"📈 Found {len(transactions)} transactions in this batch")
                
                # Let's examine the first transaction if it exists
                if transactions:
                    print(f"🔎 First transaction sample:")
                    print(json.dumps(transactions[0], indent=2))
                else:
                    print("❌ No transactions in this batch")
            else:
                print("❌ No 'data' key in response")
                print(f"Full response: {json.dumps(data, indent=2)}")

        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            print(f"Raw response text: {response.text}")
            break

        if not transactions:
            print("✅ No more records found, stopping...")
            break

        all_results.extend(transactions)
        skip += batch_size

        # Safety break for debugging
        if skip >= 2000:  # Limit to first 2 batches for debugging
            print("🛑 Debug limit reached (2000 records)")
            break

    print(f"\n📊 Total results collected: {len(all_results)}")
    return all_results

# Define liquidation query (you'll need to adjust this based on your subgraph schema)
LIQUIDATION_QUERY = """
query getLiquidations($skip: Int!) {
    liquidations(
        orderBy: timestamp_
        orderDirection: desc
        first: 1000
        skip: $skip
    ) {
        timestamp_
        borrower
        debt
        coll
        liquidator
        transactionHash_
        block_number
    }
}
"""

# Alternative queries to try if the above doesn't work
ALTERNATIVE_QUERIES = {
    "troveLiquidated": """
    query getTroveLiquidated($skip: Int!) {
        troveLiquidateds(
            orderBy: timestamp_
            orderDirection: desc
            first: 1000
            skip: $skip
        ) {
            timestamp_
            borrower
            debt
            coll
            liquidator
            transactionHash_
            block_number
        }
    }
    """,
    
    "liquidationCall": """
    query getLiquidationCalls($skip: Int!) {
        liquidationCalls(
            orderBy: timestamp_
            orderDirection: desc
            first: 1000
            skip: $skip
        ) {
            timestamp_
            borrower
            debt
            coll
            liquidator
            transactionHash_
            block_number
        }
    }
    """
}

def test_liquidation_queries():
    """Test different possible liquidation query structures"""
    
    # Try the Trove Manager subgraph first (most likely to have liquidations)
    trove_manager_client = SubgraphClient(
        url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )
    
    print("🎯 Testing MUSD Trove Manager subgraph...")
    
    # Test main liquidation query
    try:
        results = debug_fetch_subgraph_data(trove_manager_client, LIQUIDATION_QUERY, 'liquidations')
        if results:
            df = pd.DataFrame(results)
            print(f"✅ Success with 'liquidations': {len(df)} rows")
            return df
    except Exception as e:
        print(f"❌ Failed with 'liquidations': {e}")
    
    # Test alternative queries
    for method_name, query in ALTERNATIVE_QUERIES.items():
        try:
            method_plural = method_name + 's' if not method_name.endswith('s') else method_name
            results = debug_fetch_subgraph_data(trove_manager_client, query, method_plural)
            if results:
                df = pd.DataFrame(results)
                print(f"✅ Success with '{method_plural}': {len(df)} rows")
                return df
        except Exception as e:
            print(f"❌ Failed with '{method_plural}': {e}")
    
    # Also try the Borrower Operations subgraph
    print("\n🎯 Testing Borrower Operations subgraph...")
    borrower_ops_client = SubgraphClient(
        url=SubgraphClient.BORROWER_OPS_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )
    
    for method_name, query in ALTERNATIVE_QUERIES.items():
        try:
            method_plural = method_name + 's' if not method_name.endswith('s') else method_name
            results = debug_fetch_subgraph_data(borrower_ops_client, query, method_plural)
            if results:
                df = pd.DataFrame(results)
                print(f"✅ Success with '{method_plural}': {len(df)} rows")
                return df
        except Exception as e:
            print(f"❌ Failed with '{method_plural}': {e}")
    
    print("❌ No liquidation data found with any query structure")
    return pd.DataFrame()

def inspect_subgraph_schema():
    """Helper function to inspect what entities are available in your subgraph"""
    
    introspection_query = """
    query IntrospectionQuery {
        __schema {
            queryType {
                fields {
                    name
                    type {
                        name
                        kind
                    }
                }
            }
        }
    }
    """
    
    trove_manager_client = SubgraphClient(
        url=SubgraphClient.MUSD_TROVE_MANAGER_SUBGRAPH, 
        headers=SubgraphClient.SUBGRAPH_HEADERS
    )
    
    response = requests.post(
        url=trove_manager_client.url,
        headers=trove_manager_client.headers,
        json={"query": introspection_query}
    )
    
    if response.status_code == 200:
        data = response.json()
        fields = data.get('data', {}).get('__schema', {}).get('queryType', {}).get('fields', [])
        
        print("🔍 Available entities in MUSD Trove Manager subgraph:")
        liquidation_related = []
        for field in fields:
            field_name = field['name']
            print(f"  - {field_name}")
            if any(keyword in field_name.lower() for keyword in ['liquid', 'trove', 'debt', 'coll']):
                liquidation_related.append(field_name)
        
        print(f"\n🎯 Potentially liquidation-related entities: {liquidation_related}")
        return liquidation_related
    else:
        print(f"❌ Schema introspection failed: {response.status_code}")
        return []

# Run the troubleshooting
if __name__ == "__main__":
    print("🚀 Starting liquidation data troubleshooting...")
    
    # First, inspect the schema to see what's available
    print("\n" + "="*50)
    print("STEP 1: Schema Inspection")
    print("="*50)
    available_entities = inspect_subgraph_schema()
    
    # Then test the queries
    print("\n" + "="*50)
    print("STEP 2: Query Testing")
    print("="*50)
    liquidation_df = test_liquidation_queries()
    
    if not liquidation_df.empty:
        print(f"\n🎉 Successfully retrieved {len(liquidation_df)} liquidation records!")
        print("\nDataFrame info:")
        print(liquidation_df.info())
        print("\nFirst few rows:")
        print(liquidation_df.head())
    else:
        print("\n❌ No liquidation data retrieved")
        print("\n💡 Suggestions:")
        print("1. Check if liquidations actually exist in your protocol")
        print("2. Verify the correct subgraph endpoint")
        print("3. Check the entity names in your subgraph schema")
        print("4. Look at the GraphQL playground for your subgraph")