TOKEN_MAP = {
    '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599' : 'WBTC',
    '0x18084fba666a33d37592fa2633fd49a74dd93a88' : 'tBTC',
    '0xc96de26018a54d51c097160568752c4e3bd6c364' : 'FBTC',
    '0xd9d920aa40f578ab794426f5c90f6c731d159def' : 'xSolvBTC',
    '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf' : 'cbBTC',
    '0x7a56e1c57c7475ccf742a1832b028f0456652f97' : 'SolvBTC',
    '0x8db2350d78abc13f5673a411d4700bcf87864dde' : 'swBTC',
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' : 'USDC',
    '0x4c9EDD5852cd905f086C759E8383e09bff1E68B3' : 'USDe',
    '0x6b175474e89094c44da98b954eedeac495271d0f' : 'DAI',
    '0xcdf7028ceab81fa0c6971208e83fa7872994bee5' : 'T', 
    '0xdac17f958d2ee523a2206206994597c13d831ec7' : 'USDT'
 }

TOKEN_TYPE_MAP = {
    'BTC' : 'bitcoin',
    'WBTC' : 'bitcoin',
    'tBTC' : 'bitcoin',
    'FBTC' : 'bitcoin',
    'SolvBTC' : 'bitcoin',
    'xSolvBTC' : 'bitcoin',
    'swBTC' : 'bitcoin',
    'crv-stBTC' : 'bitcoin',
    'cbBTC' : 'bitcoin',
    'LBTC' : 'bitcoin',
    'intBTC' : 'bitcoin',
    'USDC': 'stablecoin',
    'USDT' : 'stablecoin',
    'USDe' : 'stablecoin',
    'crvUSD' : 'stablecoin',
    'thUSD' : 'stablecoin',
    'DAI' : 'stablecoin',
    'T' : 'ethereum'
}

TOKENS_ID = 'bitcoin,wrapped-bitcoin,tbtc,ignition-fbtc,solv-btc,solv-protocol-solvbtc-bbn,swell-restaked-btc,coinbase-wrapped-btc,lombard-staked-btc,usd-coin,tether,ethena-usde,crvusd,threshold-usd,dai,threshold-network-token'

TOKENS_ID_MAP = {
    'BTC' : 'bitcoin',
    'WBTC' : 'wrapped-bitcoin',
    'tBTC' : 'tbtc',
    'FBTC' : 'ignition-fbtc',
    'SolvBTC' : 'solv-btc',
    'xSolvBTC' : 'solv-protocol-solvbtc-bbn',
    'swBTC' : 'swell-restaked-btc',
    'cbBTC' : 'coinbase-wrapped-btc',
    'LBTC' : 'lombard-staked-btc',
    'USDC' : 'usd-coin',
    'USDT' : 'tether',
    'USDe' : 'ethena-usde',
    'crvUSD' : 'crvusd',
    'thUSD' : 'threshold-usd',
    'DAI' : 'dai',
    'T' : 'threshold-network-token'
}

POOLS_MAP = {
    '0x52e604c44417233b6ccedddc0d640a405caacefb': 'btc_musd_pool',
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': 'musdc_musd_pool',
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'musd_musdt_pool'
}

POOL_TOKEN0_MAP = {
    '0x52e604c44417233b6ccedddc0d640a405caacefb': 'BTC',
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': 'USDC',
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'MUSD'
}

# Pool token pair mappings: token0 and token1 for each pool
POOL_TOKEN_PAIRS = {
    '0x52e604c44417233b6ccedddc0d640a405caacefb': {'token0': 'BTC', 'token1': 'MUSD'},
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': {'token0': 'USDC', 'token1': 'MUSD'},
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': {'token0': 'MUSD', 'token1': 'USDT'}
}

MEZO_TOKEN_ADDRESSES = {
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'USDT',
    '0x52e604c44417233b6ccedddc0d640a405caacefb': 'BTC',
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': 'USDC',
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'MUSD'
}

MUSD_MARKET_MAP = {
    '0x28D351135955bc98f6C62535e6816399134e6506' : 'Brink',
    '0x77DCB767ae72d6aff6B8C20c2a76C6f66A5Cb46c' : 'SheFi',
    '1001' : 'ledger_nano_x',
    '1002' : 'ledger_stax', 
    '1003' : 'bitrefill_25',
    '1004' : 'bitrefill_50',
    '1005' : 'bitrefill_100', 
    '1006' : 'bitrefill_200'
}

# Token mapping for Tigris pools
TIGRIS_MAP = {
    'Volatile AMM - BTC/MUSD': 'btc_musd_pool',
    'Stable AMM - mUSDC/MUSD': 'musdc_musd_pool',
    'Stable AMM - MUSD/mUSDT': 'musd_musdt_pool',
    'Stable AMM - mUSDC/mUSDT': 'musdc_musdt_pool'
}