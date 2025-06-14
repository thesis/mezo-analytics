
class MUSDQueries:

  GET_MUSD_COLL_SNAPSHOT = """
    query getSystemCollSnapshot {
      systemSnapshotsUpdateds {
        timestamp_
        totalCollateralSnapshot
        totalStakesSnapshot
      }
    }
    """

  GET_MUSD_LIQUIDATIONS = """
    query getLiquidations($skip: Int!) {
        liquidations(
            orderBy: timestamp_
            orderDirection: desc
            first: 1000
            skip: $skip
        ) {  
            timestamp_
            liquidatedPrincipal
            liquidatedInterest
            liquidatedColl
            transactionHash_
        }
    }
    """
  
  GET_LIQUIDATED_TROVES = """
    query getTroveLiquidated($skip: Int!) {
        troveLiquidateds(
            orderBy: timestamp_
            orderDirection: desc
            first: 1000
            skip: $skip
        ) {
            id
            timestamp_
            borrower
            debt
            coll
            transactionHash_
            block_number
        }
    }
    """

  GET_LOANS = """
  query getUpdatedMusd($skip: Int!) {
    troveUpdateds(orderBy: timestamp_, orderDirection: desc, first: 1000, skip: $skip) {
      timestamp_
      borrower
      principal
      coll
      stake
      interest
      operation
      transactionHash_
      block_number
    }
  }
  """

  GET_MARKET_DONATIONS = """
    query getMarketDonations($skip: Int!) {
      donateds(first: 1000, orderBy: timestamp_, orderDirection: desc, skip: $skip) {
        timestamp_
        recipient
        amount
        donor
        transactionHash_
        block_number
      }
    }
    """

  GET_MARKET_PURCHASES = """
    query getMarketPurchases($skip: Int!) {
      orderPlaceds(first: 1000, orderBy: timestamp_, orderDirection: desc, skip: $skip) {
        timestamp_
        productId
        price
        customer
        orderId
        transactionHash_
        block_number
      }
    }
    """

class BridgeQueries:

  GET_AUTOBRIDGE_TRANSACTIONS = """
  query autobridgeDeposits($skip: Int!) {
    depositAutoBridgeds (
      orderBy: timestamp_
      orderDirection: desc
      first: 1000
      skip: $skip
    ) {
      timestamp_
      amount
      token
      depositor
      transactionHash_
      block_number
      depositId
    }
  }
  """

  GET_BRIDGE_TRANSACTIONS = """
  query getBridgedAssets($skip: Int!) {
    assetsLockeds(
      orderBy: timestamp_ 
      orderDirection: desc
      first: 1000
      skip: $skip
    ) {
      timestamp_
      amount
      token
      recipient
      transactionHash_
    }
  }
  """
  
  GET_WITHDRAWALS = """
  query withdrawnDeposits($skip: Int!) {
    withdrawns(
      orderBy: timestamp_ 
      orderDirection: desc
      first: 1000
      skip: $skip
    ) {
      timestamp_
      amount
      token
      depositor
      transactionHash_
      block_number
      depositId
    }
  }
  """