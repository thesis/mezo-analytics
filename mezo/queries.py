
class MUSDQueries:

  GET_TROVE_CREATED = """
  query getMusdTxns($skip: Int!) {
    troveCreateds(orderBy: timestamp_, orderDirection: desc, first: 1000, skip: $skip) {
      timestamp_
      borrower
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

  GET_NEW_TROVES = """
  query getUpdatedLoans($skip: Int!) {
    troveUpdateds(
      orderBy: timestamp_
      orderDirection: desc
      first: 1000
      skip: $skip
      where: {operation: "0"}
    ) {
      timestamp_
      borrower
      principal
      coll
      stake
      interest
      operation
      transactionHash_
    }
  }
  """

  GET_UPDATED_TROVES = """
  query getUpdatedLoans($skip: Int!) {
    troveUpdateds(
      orderBy: timestamp_
      orderDirection: desc
      first: 1000
      skip: $skip
      where: {operation: "2"}
    ) {
      timestamp_
      borrower
      principal
      coll
      stake
      interest
      operation
      transactionHash_
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
  query bridgedDeposits($skip: Int!) {
    depositBridgeds(
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