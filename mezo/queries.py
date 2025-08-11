
class PoolQueries:
  GET_DEPOSITS = """
  query getMints ($skip: Int!) {
    mints(
        first: 1000
        orderBy: timestamp_
        orderDirection: desc
        skip: $skip
    ) {
        timestamp_
        sender
        amount0
        amount1
        contractId_
        transactionHash_
    }
}
"""

  GET_WITHDRAWALS = """
  query getWithdrawals($skip: Int!) {
    burns(
      first: 1000
      orderBy: timestamp_
      orderDirection: desc
      skip: $skip
  ) {
      timestamp_
      sender
      to
      amount0
      amount1
      contractId_
      transactionHash_
    }
  }
  """

  GET_POOL_VOLUME = """
  query getPoolVolumes($skip: Int!) {
    poolVolumes(
      interval: day
      first: 1000
      orderBy: timestamp_ 
      orderDirection: desc
      skip: $skip
    ) {
      timestamp
      pool {
        name
        token0 {
          symbol
        }
        token1 {
          symbol
        }
      }
      totalVolume0
      totalVolume1
      id
    }
  }
  """

  GET_TOTAL_POOL_FEES = """
  query getTotalPoolFees($skip: Int!) {
    feesStats_collection(
      interval: day
      first: 1000
      orderBy: timestamp_ 
      orderDirection: desc
      skip: $skip
    ) {
      timestamp
        pool {
        name
          token0 {
            symbol
          }
          token1 {
            symbol
          }
        }
      totalFees0
      totalFees1
      id
    }
  }
  """

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
  
  GET_SWAPS = """
    query GetSwaps($skip: Int!) {
      swaps(
      first: 1000
      orderBy: timestamp_
      orderDirection: desc
      skip: $skip
      ) {
        timestamp_
        sender
        to
        contractId_
        amount0In
        amount0Out
        amount1In
        amount1Out
        transactionHash_
        block_number
      }
    }
    """
  
  GET_MUSD_PRICE = """
  query getMusdPrice($skip: Int!) {
    syncs(
      orderBy: timestamp_
      orderDirection: desc
      first: 1000
      skip: $skip
    ) {
      timestamp_
      reserve0
      reserve1
      contractId_
      transactionHash_
      block_number
    }
  }
"""

  GET_REDEMPTIONS = """
  query getRedemptions($skip: Int!) {
      redemptions(
      first: 1000
      orderBy: timestamp_
      orderDirection: desc
      skip: $skip
    ) {
      timestamp_
      actualAmount
      attemptedAmount
      collateralFee
      collateralSent
      transactionHash_
      block_number
    }
  }
  """

  GET_BORROW_FEES = """
  query getBorrowFees ($skip: Int!) {
    borrowingFeePaids (
      orderBy: timestamp_
      orderDirection: desc
      first: 1000
      skip: $skip
    ){
      timestamp_
      fee
      borrower
      transactionHash_
    }
  }
  """

class BridgeQueries:

  GET_WORMHOLE_TXNS = """
  query getWormholeTxns {
    transferSents(first: 10, orderBy: timestamp_, orderDirection: desc) {
      timestamp_
      refundAddress
      recipient
      recipientChain
      msgSequence
      amount
      fee
      contractId_
      transactionHash_
    }
  }
  """

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

class VaultQueries:
  GET_VAULT_TRANSFERS = """
  query vaultTransfers($skip: Int!) {
    transfers(
      first: 1000
      orderBy: timestamp_
      orderDirection: desc
      skip: $skip
  ) {
      timestamp_
      from
      to
      value
      transactionHash_
      block_number
    }
  }
  """