from datetime import datetime
from decimal import Decimal
from typing import Dict
from unittest import TestCase

from ..database import DataBase, DataBaseAPI, APIBase
from ..Dataclasses import Coin, ProtoTransaction, Transaction, TransactionType


class MockExternalAPI(APIBase):

    def __init__(self):
        self._cached_data: Dict[str, Decimal] = {}

    def addFakeCacheData(self, symbol: str, date: datetime, value: Decimal):
        timestamp = str(int(date.timestamp() * 1000))
        cached_id = f"{symbol}_{timestamp}"
        self._cached_data[cached_id] = value

    def get_conversion_rate(self, first: str, second: str, date: datetime = None) -> Decimal:
        symbol = first + second
        timestamp = str(int(date.timestamp() * 1000))
        cached_id = f"{symbol}_{timestamp}"
        return self._cached_data[cached_id]


class TestDataBaseAPI(TestCase):

    def setUp(self):
        self.db = DataBaseAPI.create_new_database('test')
        self.external_api = MockExternalAPI()
        self.api = DataBaseAPI(self.external_api, self.db)

    def _create_buy_proto_transaction(self, first, second, quantity, datetime, cost_per_coin):
        proto = ProtoTransaction(quantity, first, ProtoTransaction.TransactionType.BUY, datetime, 'test')
        self.external_api.addFakeCacheData(first + second, datetime, cost_per_coin)
        return proto

    def test_create_database(self):
        db = DataBaseAPI.create_new_database('test')

        assert isinstance(db, DataBase)

    def test_create_API(self):
        db = DataBaseAPI.create_new_database('test')
        api = DataBaseAPI(MockExternalAPI(), db)

        assert isinstance(api, DataBaseAPI)

    def test_add_coin(self):
        new_coin = self.api.add_coin('BTC')

        assert isinstance(new_coin, Coin)
        assert new_coin.coin_info.tick == 'BTC'
        assert new_coin is self.api.get_coin('BTC')

    def test_add_existing_coin(self):
        self.api.add_coin('BTC')
        with self.assertRaises(KeyError):
            self.api.add_coin('BTC')

    def test_not_existing_coin(self):
        with self.assertRaises(KeyError):
            self.api.add_coin('adafdgag')

    def test_remove_coin(self):
        self.api.add_coin('BTC')

        self.api.remove_coin('BTC')

        with self.assertRaises(KeyError):
            self.api.get_coin('BTC')

        with self.assertRaises(KeyError):
            self.api.remove_coin('BTC')

    def test_add_coin_validating_transaction(self):
        time = datetime.now()
        self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))

        coin = self.api.get_coin('BTC')

        assert coin
        assert coin.coin_info.tick == 'BTC'

    def test_validate_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))

        valid_transactions = self.api.validate_import([proto1])

        trans = valid_transactions[0]

        assert isinstance(trans, Transaction)
        assert isinstance(trans.operation_type, Transaction.TransactionType.BUY)
        assert trans.coin == 'BTC'
        assert trans.quantity == Decimal(10)
