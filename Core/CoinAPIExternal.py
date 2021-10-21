from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Callable
from decimal import Decimal

import pandas as pd
from binance.client import Client

from .Dataclasses import CoinInfo


class APIBase:

    def get_conversion_rate(self, first: str, second: str, date: datetime = None) -> Decimal:
        raise NotImplementedError


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


class BinanceAPI(APIBase):
    @dataclass
    class Coin:
        coin_tick: str
        coin_pairs: Dict[str, 'BinanceAPI.Pair']

    @dataclass
    class Pair:
        symbol: str
        first: 'BinanceAPI.Coin'
        second: 'BinanceAPI.Coin'
        inv_symbol: str = field(init=False)

        def __post_init__(self):
            self.inv_symbol = self.second.coin_tick + self.first.coin_tick

    class PriceHistoryDatabase:

        def __init__(self, path: Path, request_callback: Callable[[str, datetime], str]):
            self._cache_path = path
            self._cached_data = {}
            self._request_price = request_callback

            self._load_cached_data()

            self.file_handler = self._cache_path.open('a')

        def __del__(self):
            self.file_handler.close()

        def _load_cached_data(self):
            if not self._cache_path.exists():
                return None

            with self._cache_path.open('r') as f:
                print("Loading price cache from file")
                for row in f.readlines():
                    id, value = row.split(';')
                    self._cached_data[id] = value

        def _add_to_cache(self, cached_id: str, value: str):
            self._cached_data[cached_id] = value
            self.file_handler.write(f"{cached_id};{value}\n")

        def get_price(self, symbol, date: datetime) -> Decimal:
            timestamp = str(int(date.timestamp() * 1000))
            cached_id = f"{symbol.symbol}_{timestamp}"
            if cached_id in self._cached_data:
                print(f"Using cache: {cached_id}")
                return Decimal(self._cached_data[cached_id])
            else:
                print(f"Requesting price: {cached_id}")
                value = self._request_price(symbol.symbol, date)
                self._add_to_cache(cached_id, value)
                return Decimal(value)

    class LimitCheck:
        """Unused"""

        def __init__(self):
            self._used_weight = 0
            self._last_minute = datetime.now()
            self._total_calls = 0

    def __init__(self, keys_path, cache_folder):

        keys = self._readKeys(keys_path)

        self._cache_validity = timedelta(days=15)
        self._cache_folder_path = Path(cache_folder)
        self._check_cache_folder(self._cache_folder_path)
        self._cache_pairs_path = self._cache_folder_path / "pairs_cache.csv"

        self._client = Client(**keys)
        self._client.ping()

        self._coin_dict = {}
        self._pairs_priority = ('BTC', 'ETH', 'BNB', 'BUSD', 'USDT')

        self._cache_price_path = self._cache_folder_path / "price_history.txt"
        self._price_history_db = self.PriceHistoryDatabase(self._cache_price_path, self._get_price)

        symbols_dataframe = self._check_pairs_cache(self._cache_pairs_path)
        self._build_pairs(symbols_dataframe)

    def _get_price(self, symbol: str, target_time: datetime) -> str:

        start_time = target_time - timedelta(seconds=30)
        start_time_timestamp = int(start_time.timestamp() * 1000)
        end_time = target_time + timedelta(seconds=30)
        end_time_timestamp = int(end_time.timestamp() * 1000)

        data = self._client.get_historical_klines(symbol, self._client.KLINE_INTERVAL_1MINUTE,
                                                  start_time_timestamp,
                                                  end_time_timestamp)

        kline = data[0]
        average = (Decimal(kline[1]) + Decimal(kline[4])) * Decimal(0.5)
        return average

    def get_conversion_rate(self, first: str, second: str, date: datetime = None) -> Decimal:
        return self._conversion(self.Pair(first + second, self._get_coin(first), self._get_coin(second)), date)

    def _conversion(self, pair: Pair, date: datetime) -> Decimal:
        coin = pair.first
        symbol = pair.symbol
        inv_symbol = pair.inv_symbol
        if symbol in coin.coin_pairs:
            return self._get_conversion(coin.coin_pairs[symbol], date)
        elif inv_symbol in coin.coin_pairs:
            return Decimal(1) / self._get_conversion(coin.coin_pairs[symbol], date)
        else:
            for possible in self._pairs_priority:
                if pair.first.coin_tick + possible in coin.coin_pairs:
                    return self._get_conversion(coin.coin_pairs[pair.first.coin_tick + possible], date) * \
                           self._conversion(self.Pair(possible + pair.second.coin_tick,
                                                      self._get_coin(possible),
                                                      pair.second),
                                            date)
            raise ValueError(f"Not conversion found for {pair.first.coin_tick} and {pair.second.coin_tick}")

    def _get_coin(self, coin_name: str) -> Coin:
        try:
            return self._coin_dict[coin_name]
        except KeyError:
            raise KeyError(f"The coin {coin_name} doesn't exist in any pair")

    def _get_conversion(self, symbol: Pair, date) -> Decimal:
        return self._price_history_db.get_price(symbol, date)

    def _check_cache_folder(self, folder_path: Path):
        if not folder_path.exists():
            raise FileNotFoundError("Cache folder doesn't exist")

    def _check_pairs_cache(self, pairs_path: Path) -> pd.DataFrame:
        if pairs_path.exists() and self._cache_is_valid(pairs_path):
            print("Loading cached pairs data")
            pairs_dataframe = self._load_pair_data(pairs_path)
        else:
            print("Retrieving pairs data")
            pairs_dataframe = self._retrieve_pairs_data()
            self._save_pair_data(pairs_dataframe, pairs_path)
        return pairs_dataframe

    def _cache_is_valid(self, path: Path):
        with path.open('r') as fp:
            date = datetime.strptime(fp.readline().strip(), "%Y%m%d")
            if datetime.now() - date > self._cache_validity:
                return False
        return True

    def _retrieve_pairs_data(self) -> pd.DataFrame:
        data = self._client.get_exchange_info()

        symbols_list = data['symbols']

        dataframe_temp_list = []

        number_of_symbols = len(symbols_list)
        for idx, symbol in enumerate(symbols_list):
            if idx % 100 == 0:
                print(f"Adding symbol {idx} of {number_of_symbols}")
            dataframe_temp_list.append((symbol['symbol'], symbol['baseAsset'], symbol['quoteAsset']))

        return pd.DataFrame(dataframe_temp_list, columns=['Symbol', 'First', 'Second'])

    def _build_pairs(self, symbols_dataframe: pd.DataFrame):
        for _, row in symbols_dataframe.iterrows():
            coin_first = self._get_or_create_coin(row['First'])
            coin_second = self._get_or_create_coin(row['Second'])
            pair = self.Pair(row['Symbol'], coin_first, coin_second)
            coin_first.coin_pairs[pair.symbol] = pair

    def _get_or_create_coin(self, coin_name):
        if coin_name not in self._coin_dict:
            coin = self.Coin(coin_name, {})
            self._coin_dict[coin.coin_tick] = coin
        else:
            coin = self._coin_dict[coin_name]
        return coin

    def _save_pair_data(self, symbols_dataframe: pd.DataFrame, path: Path):
        with path.open('w') as fp:
            fp.write(f'{datetime.now().strftime("%Y%m%d")}\n')
            symbols_dataframe.to_csv(fp)

    def _load_pair_data(self, path: Path) -> pd.DataFrame:
        with path.open('r') as fp:
            _ = fp.readline()
            return pd.read_csv(fp)

    def _readKeys(self, path):
        with open(path, 'r') as f:
            api_key = f.readline().strip()
            api_secret = f.readline().strip()
        return {'api_key': api_key,
                'api_secret': api_secret}

    # def getCoinInfo(self, coin_name):
    #     return self._client.get_symbol_info(coin_name)
