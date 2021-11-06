from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict
from unittest import TestCase, mock
from freezegun import freeze_time

from ..database import DataBase, DataBaseAPI, APIBase
from ..Dataclasses import Coin, ProtoTransaction, Transaction


class MockExternalAPI(APIBase):

    def __init__(self):
        self._cached_data: Dict[str, Decimal] = {}

    def add_fake_cache_data(self, symbol: str, date: datetime, value: Decimal):
        timestamp = str(int(date.timestamp() * 1000))
        cached_id = f"{symbol}_{timestamp}"
        self._cached_data[cached_id] = value

    def get_conversion_rate(self, first: str, second: str, date: datetime = None) -> Decimal:
        symbol = first + second
        timestamp = str(int(date.timestamp() * 1000))
        cached_id = f"{symbol}_{timestamp}"
        return self._cached_data[cached_id]


def get_now_time():
    return datetime(2021, 1, 1, 0, 0, 0)


@freeze_time("2021-01-01 00:00:00")
class TestDataBaseAPI(TestCase):

    def setUp(self):
        self.db = DataBaseAPI.create_new_database('test')
        self.external_api = MockExternalAPI()
        self.api = DataBaseAPI(self.external_api, self.db)

    def _create_buy_proto_transaction(self, first, second, quantity, date, cost_per_coin):
        proto = ProtoTransaction(quantity, first, ProtoTransaction.TransactionType.BUY, date, 'test')
        self.external_api.add_fake_cache_data(first + second, date, cost_per_coin)
        return proto

    def _create_BTC_buy_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.BUY, date, 'test')

    def _create_BTC_sell_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.SELL, date, 'test')

    def _create_BTC_deposit_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.DEPOSIT, date, 'test')

    def _create_BTC_fee_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.FEE, date, 'test')

    def _create_BTC_pos_int_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.POS_INTEREST, date, 'test')

    def _create_BTC_pos_redem_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.POS_REDEMPTION, date, 'test')

    def _create_BTC_pos_purchase_proto(self, quantity, date):
        return ProtoTransaction(quantity, 'BTC', ProtoTransaction.TransactionType.POS_PURCHASE, date, 'test')

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
            self.api.add_coin('casa')

    def test_remove_coin(self):
        self.api.add_coin('BTC')

        self.api.remove_coin('BTC')

        with self.assertRaises(KeyError):
            self.api.get_coin('BTC')

        with self.assertRaises(KeyError):
            self.api.remove_coin('BTC')

    def test_add_coin_validating_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))

        self.api.validate_import([proto1])

        coin = self.api.get_coin('BTC')

        assert coin
        assert coin.coin_info.tick == 'BTC'

    def test_validate_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))

        valid_transactions = self.api.validate_import([proto1])

        trans = valid_transactions[0]

        assert isinstance(trans, Transaction)
        assert trans.operation_type == Transaction.TransactionType.BUY
        assert trans.coin.coin_info.tick == 'BTC'
        assert trans.quantity == Decimal(10)

    def test_invalid_value_in_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(-10), 'BTC', ProtoTransaction.TransactionType.DEPOSIT, time, 'test')]

        with self.assertRaises(AssertionError):
            self.api.validate_import(proto_list)

    def test_invalid_value_out_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.SELL, time, 'test')]

        with self.assertRaises(AssertionError):
            self.api.validate_import(proto_list)

    def test_duplicate_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test'),
                      ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test')]

        with self.assertRaises(ValueError):
            self.api.validate_import(proto_list)

    def test_add_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))
        valid_transactions = self.api.validate_import([proto1])
        self.api.add_transaction(valid_transactions)

        coin = self.api.get_coin('BTC')

        assert len(coin.transactions)
        assert coin.transactions[0] is valid_transactions[0]

    def test_coin_data_generation(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))
        valid_transactions = self.api.validate_import([proto1])
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = []
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data
        assert coin_data.coin.coin_info.tick == 'BTC'

    def test_coin_data_group_transactions(self):
        time_start = datetime.now() - timedelta(days=10)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_sell_proto(Decimal(-2), time_start),
                      self._create_BTC_sell_proto(Decimal(-2), time_start + timedelta(days=1)),
                      self._create_BTC_fee_proto(Decimal(-1), time_start),
                      self._create_BTC_deposit_proto(Decimal(50), time_start),
                      self._create_BTC_pos_purchase_proto(Decimal(-10), time_start),
                      self._create_BTC_pos_int_proto(Decimal(1), time_start),
                      self._create_BTC_pos_int_proto(Decimal(2), time_start + timedelta(days=1)),
                      self._create_BTC_pos_int_proto(Decimal(1), time_start + timedelta(days=2)),
                      self._create_BTC_pos_redem_proto(Decimal(2), time_start)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_earnings]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert len(coin_data.get_transactions([Transaction.TransactionType.BUY])) == 1
        assert len(coin_data.get_transactions([Transaction.TransactionType.SELL])) == 2
        assert len(coin_data.get_transactions([Transaction.TransactionType.FEE])) == 1
        assert len(coin_data.get_transactions([Transaction.TransactionType.DEPOSIT])) == 1
        assert len(coin_data.get_transactions([Transaction.TransactionType.POS_PURCHASE])) == 1
        assert len(coin_data.get_transactions([Transaction.TransactionType.POS_INTEREST])) == 3
        assert len(coin_data.get_transactions([Transaction.TransactionType.POS_REDEMPTION])) == 1

    def test_coin_data_current_value(self):
        time_now = datetime.now()
        time = time_now - timedelta(days=1)
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))
        self.external_api.add_fake_cache_data('BTCEUR', time_now, Decimal(2))

        valid_transactions = self.api.validate_import([proto1])
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_current_value_per_coin]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data.current_value_per_unit == Decimal(2)

    def test_coin_data_spot_quantities(self):
        time_now = datetime.now()
        self.external_api.add_fake_cache_data('BTCEUR', time_now, Decimal(1))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_now),
                      self._create_BTC_sell_proto(Decimal(-2), time_now),
                      self._create_BTC_fee_proto(Decimal(-1), time_now),
                      self._create_BTC_deposit_proto(Decimal(50), time_now),
                      self._create_BTC_pos_purchase_proto(Decimal(-10), time_now),
                      self._create_BTC_pos_int_proto(Decimal(1), time_now)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_spot_quantities]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data._spot_quantity == Decimal(48)

    def test_coin_data_earn_quantities(self):
        time_now = datetime.now()
        self.external_api.add_fake_cache_data('BTCEUR', time_now, Decimal(1))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_now),
                      self._create_BTC_sell_proto(Decimal(-2), time_now),
                      self._create_BTC_fee_proto(Decimal(-1), time_now),
                      self._create_BTC_deposit_proto(Decimal(50), time_now),
                      self._create_BTC_pos_purchase_proto(Decimal(-10), time_now),
                      self._create_BTC_pos_int_proto(Decimal(1), time_now),
                      self._create_BTC_pos_redem_proto(Decimal(2), time_now)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_spot_quantities, self.api._compute_earn_quantities]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data._spot_quantity == Decimal(50)
        assert coin_data._earn_quantity == Decimal(8)

    def test_coin_data_fees_quantities(self):
        time_now = datetime.now()
        self.external_api.add_fake_cache_data('BTCEUR', time_now, Decimal(10))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_now),
                      self._create_BTC_sell_proto(Decimal(-2), time_now),
                      self._create_BTC_fee_proto(Decimal(-1), time_now),
                      self._create_BTC_deposit_proto(Decimal(50), time_now),
                      self._create_BTC_pos_purchase_proto(Decimal(-10), time_now),
                      self._create_BTC_pos_int_proto(Decimal(1), time_now),
                      self._create_BTC_pos_redem_proto(Decimal(2), time_now)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_fees_quantities]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert len(coin_data._fees_transactions) == 1
        fee_trans = coin_data._fees_transactions[0]
        assert fee_trans.transaction.quantity == Decimal(-1)
        assert fee_trans.cost_per_unit == Decimal(10)
        assert fee_trans.cost == Decimal(-10)

    def test_coin_data_earnings(self):
        time_start = datetime.now() - timedelta(days=10)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_sell_proto(Decimal(-2), time_start),
                      self._create_BTC_fee_proto(Decimal(-1), time_start),
                      self._create_BTC_deposit_proto(Decimal(50), time_start),
                      self._create_BTC_pos_purchase_proto(Decimal(-10), time_start),
                      self._create_BTC_pos_int_proto(Decimal(1), time_start),
                      self._create_BTC_pos_int_proto(Decimal(2), time_start + timedelta(days=1)),
                      self._create_BTC_pos_int_proto(Decimal(1), time_start + timedelta(days=2)),
                      self._create_BTC_pos_redem_proto(Decimal(2), time_start)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_earnings]
        self.api.process_coin_data('BTC')

        coin_earn = self.api.get_coin_data('BTC')._coin_earn

        assert coin_earn.quantity == Decimal(4)
        assert coin_earn.current_conversion_rate == Decimal(50)
        assert coin_earn.current_value == Decimal(200)

    def test_coin_data_gains_single_buy(self):
        time_start = datetime.now() - timedelta(days=10)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start)]

        valid_transactions = self.api.validate_import(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.active_processes = [self.api._compute_gains]
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert len(coin_data._buy_transactions_data) == 1
