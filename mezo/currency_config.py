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

TOKENS_ID = 'bitcoin,wrapped-bitcoin,tbtc,ignition-fbtc,solv-btc,solv-protocol-staked-btc,swell-restaked-btc,coinbase-wrapped-btc,lombard-staked-btc,usd-coin,tether,ethena-usde,crvusd,threshold-usd,dai,threshold-network-token'

TOKENS_ID_MAP = {
    'BTC' : 'bitcoin',
    'WBTC' : 'wrapped-bitcoin',
    'tBTC' : 'tbtc',
    'FBTC' : 'ignition-fbtc',
    'SolvBTC' : 'solv-btc',
    'xSolvBTC' : 'solv-protocol-staked-btc',
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
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'musd_musdt_pool',
    '0x2a1ab0224a7a608d3a992cb15594a2934f74f4c0': 'musdc_musdt_pool',
    '0xb07c2172c4b7bbc3ac52088d30cb103853b0b403': 'musdc_btc_pool',
    '0x329d64572f8922c3fe90d23a3c74a360d8ea6235': 'btc_xsolvbtc_pool',
    '0x58c8f6d2e589928c46425eaf4254b6a41c45a584': 'upmusd_musd_pool',
    '0x5cd2a025c001e07ae354a4c22c3009908de1ac59': 'solvbtc_musd_pool',
    '0x9e60cd4d5b718178fab0137200a36a5472191302': 'btc_musdt_pool',
    '0xb7fd1db5228e4d9f4109c5635f66375e5af0d8f5': 'musd_xsolvbtc_pool',
    '0xf6f950485b0a65828f07581ca979ef1271778d6a': 'btc_solvbtc_pool',
    '0xfbcc89586780ac6f41e9cec97663e5592be41331': 'upmusd_btc_pool'
}

POOL_TOKEN0_MAP = {
    '0x52e604c44417233b6ccedddc0d640a405caacefb': 'BTC',
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': 'USDC',
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'MUSD',
}

POOL_TOKEN_PAIRS = {
    '0x52e604c44417233b6ccedddc0d640a405caacefb': {'token0': 'BTC', 'token1': 'MUSD'},
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': {'token0': 'USDC', 'token1': 'MUSD'},
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': {'token0': 'MUSD', 'token1': 'USDT'},
    '0x329d64572f8922c3fe90d23a3c74a360d8ea6235': {'token0': 'BTC', 'token1': 'xSolvBTC'},
    '0x2a1ab0224a7a608d3a992cb15594a2934f74f4c0': {'token0': 'USDC', 'token1': 'USDT'},
    '0x58c8f6d2e589928c46425eaf4254b6a41c45a584': {'token0': 'upMUSD', 'token1': 'MUSD'},
    '0x5cd2a025c001e07ae354a4c22c3009908de1ac59': {'token0': 'SolvBTC', 'token1': 'MUSD'},
    '0x9e60cd4d5b718178fab0137200a36a5472191302': {'token0': 'BTC', 'token1': 'USDT'},
    '0xb07c2172c4b7bbc3ac52088d30cb103853b0b403': {'token0': 'USDC', 'token1': 'BTC'},
    '0xb7fd1db5228e4d9f4109c5635f66375e5af0d8f5': {'token0': 'MUSD', 'token1': 'xSolvBTC'},
    '0xf6f950485b0a65828f07581ca979ef1271778d6a': {'token0': 'BTC', 'token1': 'SolvBTC'},
    '0xfbcc89586780ac6f41e9cec97663e5592be41331': {'token0': 'upMUSD', 'token1': 'BTC'}
}

MEZO_TOKEN_ADDRESSES = {
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'USDT',
    '0x52e604c44417233b6ccedddc0d640a405caacefb': 'BTC',
    '0xa10aD2570ea7b93d19fDae6Bd7189fF4929Bc747': 'SolvBTC',
    '0xdF708431162Ba247dDaE362D2c919e0fbAfcf9DE': 'xSolvBTC',
    '0xed812aec0fecc8fd882ac3eccc43f3aa80a6c356': 'USDC',
    '0x10906a9e9215939561597b4c8e4b98f93c02031a': 'MUSD',
    '0x221B2D9aD7B994861Af3f4c8A80c86C4aa86Bf53': 'upMUSD',
    '0xaaC423eDC4E3ee9ef81517e8093d52737165b71F': 'T', 
    '0x29fA8F46CBB9562b87773c8f50a7F9F27178261c': 'swBTC',
    '0x1531b6e3d51BF80f634957dF81A990B92dA4b154': 'DAI',
    '0x812fcC0Bb8C207Fd8D6165a7a1173037F43B2dB8': 'FBTC',
    '0xdf6542260a9F768f07030E4895083F804241F4C4': 'USDe',
    '0x6a7CD8E1384d49f502b4A4CE9aC9eb320835c5d7': 'cbBTC'
}

TIGRIS_MAP = {
    'Volatile AMM - BTC/MUSD': 'btc_musd_pool',
    'Stable AMM - mUSDC/MUSD': 'musdc_musd_pool',
    'Stable AMM - MUSD/mUSDT': 'musd_musdt_pool',
    'Stable AMM - mUSDC/mUSDT': 'musdc_musdt_pool',
    'Volatile AMM - mUSDC/BTC': 'musdc_btc_pool',
    'Stable AMM - BTC/mxSolvBTC': 'btc_xsolvbtc_pool',
    'Volatile AMM - upMUSD/MUSD': 'upmusd_musd_pool',
    'Volatile AMM - mSolvBTC/MUSD': 'solvbtc_musd_pool',
    'Volatile AMM - BTC/mUSDT': 'btc_musdt_pool',
    'Volatile AMM - MUSD/mxSolvBTC': 'musd_xsolvbtc_pool',
    'Stable AMM - BTC/mSolvBTC': 'btc_solvbtc_pool',
    'Volatile AMM - upMUSD/BTC': 'upmusd_btc_pool',
    'Volatile AMM - mT/MUSD': 'mt_musd_pool'

}

MEZO_ASSET_NAMES_MAP = {
    'mUSDC': 'USDC',
    'mUSDT': 'USDT',
    'mxSolvBTC': 'xSolvBTC',
    'mSolvBTC': 'SolvBTC',
    'mT': 'T'
}

MUSD_MARKET_MAP = {
    '0x28D351135955bc98f6C62535e6816399134e6506' : 'Brink',
    '0x77DCB767ae72d6aff6B8C20c2a76C6f66A5Cb46c' : 'SheFi',
    '1001' : 'ledger_nano_x',
    '1002' : 'ledger_stax', 
    '1003' : 'bitrefill_25',
    '1004' : 'bitrefill_50',
    '1005' : 'bitrefill_100', 
    '1006' : 'bitrefill_200',
    '1007' : 'bitrefill_1000'
}