from .Dataclasses import CoinInfo
from binance.client import Client


class CoinAPI:
    __FTM_coin_info = CoinInfo('FTM', 'Phantom')
    _coin_data = {'ADA': CoinInfo('ADA', 'Ada Cardano'),
                  'BTC': CoinInfo('BTC', 'Bitcoin'),
                  'EUR': CoinInfo('EUR', 'Euro'),
                  'FTM': __FTM_coin_info,
                  'ETH': CoinInfo('ETH', 'Ethereum'),
                  'HOT': CoinInfo('HOT', 'Holo'),
                  'ALGO': CoinInfo('ALGO', 'Algorand'),
                  'SOL': CoinInfo('SOL', 'Solana'),
                  'ENJ': CoinInfo('ENJ', 'Enjin Coin'),
                  'DOGE': CoinInfo('DOGE', 'Dogecoin'),
                  'IOTX': CoinInfo('IOTX', 'IoTex'),
                  'XMR': CoinInfo('XMR', 'Monero'),
                  'BUSD': CoinInfo('BUSD', 'BUSD'),
                  'USDT': CoinInfo('USDT', 'BUSDT'),
                  'WIN': CoinInfo('WIN', 'WINK'),
                  'GRT': CoinInfo('GRT', 'The Graph'),
                  'EZ': CoinInfo('EZ', 'EasyFi'),
                  'AUCTION': CoinInfo('AUCTION', 'Auction'),
                  'VTHO': CoinInfo('VTHO', 'VeTHor Token'),
                  'VET': CoinInfo('VET', 'VetChain'),
                  'FIL': CoinInfo('FIL', 'Filecoin'),
                  'SHIB': CoinInfo('SHIB', 'SHIBA INU'),
                  'OAX': CoinInfo('OAX', 'openANX'),
                  'KLAY': CoinInfo('KLAY', 'Klaytn'),
                  'INJ': CoinInfo('INJ', 'Injective Protocol'),
                  'BNB': CoinInfo('BNB', 'Binance Coin'),
                  'AXS': CoinInfo('AXS', 'Axis Infinite'),
                  'MATIC': CoinInfo('MATIC', 'Matic'),
                  'LDFTM': __FTM_coin_info}

    @classmethod
    def get_coin_info(cls, coin_name):
        return cls._coin_data[coin_name]


# class CoinMarketAPI:
#
#     def __init__(self):
#         api_key = "d0b1d8ff-101c-4c25-a3b9-33b66de374ed"
#
#         cmc = CoinMarketCapAPI(api_key)
#
#         r = cmc.cryptocurrency_info(symbol='BTC')
#
#         pass


class BinanceAPI:

    def __init__(self, keys_path):

        keys = self._readKeys(keys_path)

        self._client = Client(**keys)
        self._client.ping()

    def _readKeys(self, path):
        with open(path, 'r') as f:
            api_key = f.readline().strip()
            api_secret = f.readline().strip()
        return {'api_key': api_key,
                'api_secret': api_secret}
