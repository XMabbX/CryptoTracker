from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict
from unittest import TestCase

from freezegun import freeze_time

from ..Dataclasses import Coin, ProtoTransaction, Transaction
from ..database import DataBase, DataBaseAPI, APIBase, TransactionValidator


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


@freeze_time("2021-01-01 00:00:00")
class TestDataBaseAPI(TestCase):

    def setUp(self):
        self.db = DataBaseAPI.create_new_database('test')
        self.external_api = MockExternalAPI()
        self.api: DataBaseAPI = DataBaseAPI(self.db, self.external_api)
        self.validator = TransactionValidator(self.api)

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
        api = DataBaseAPI(db, MockExternalAPI())

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

        self.validator.validate_and_parse_transactions([proto1])

        coin = self.api.get_coin('BTC')

        assert coin
        assert coin.coin_info.tick == 'BTC'

    def test_validate_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))

        valid_transactions = self.validator.validate_and_parse_transactions([proto1])

        trans = valid_transactions[0]

        assert isinstance(trans, Transaction)
        assert trans.operation_type == Transaction.TransactionType.BUY
        assert trans.coin.coin_info.tick == 'BTC'
        assert trans.quantity == Decimal(10)

    def test_invalid_value_in_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(-10), 'BTC', ProtoTransaction.TransactionType.DEPOSIT, time, 'test')]

        with self.assertRaises(AssertionError):
            self.validator.validate_and_parse_transactions(proto_list)

    def test_invalid_value_out_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.POS_PURCHASE, time, 'test')]

        with self.assertRaises(AssertionError):
            self.validator.validate_and_parse_transactions(proto_list)

    def test_duplicate_transaction(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test'),
                      ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test')]

        with self.assertRaises(ValueError):
            self.validator.validate_and_parse_transactions(proto_list)

    def test_duplicate_transaction_whitelist(self):
        time = datetime.now()
        proto_list = [ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test'),
                      ProtoTransaction(Decimal(10), 'BTC', ProtoTransaction.TransactionType.BUY, time, 'test')]
        self.validator._duplicate_ids.add('0x11878b3c823c45000_0x2_BTC_10')
        new_transactions = self.validator.validate_and_parse_transactions(proto_list)
        assert len(new_transactions) == 1

    def test_add_transaction(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))
        valid_transactions = self.validator.validate_and_parse_transactions([proto1])
        self.api.add_transaction(valid_transactions)

        coin = self.api.get_coin('BTC')

        assert len(coin.transactions)
        assert coin.transactions[0] is valid_transactions[0]

    def test_coin_data_generation(self):
        time = datetime.now()
        proto1 = self._create_buy_proto_transaction('BTC', 'EUR', Decimal(10), time, Decimal(1))
        valid_transactions = self.validator.validate_and_parse_transactions([proto1])
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.COMPUTE_GAINS = False
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

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_GAINS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api._get_coin_data('BTC')

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

        valid_transactions = self.validator.validate_and_parse_transactions([proto1])
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.COMPUTE_GAINS = False
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

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.COMPUTE_GAINS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data.spot_quantity == Decimal(48)

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

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.COMPUTE_GAINS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data.spot_quantity == Decimal(50)
        assert coin_data.earn_quantity == Decimal(8)

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

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.COMPUTE_GAINS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert coin_data.fees_data.total_cost == Decimal(-10)
        assert len(coin_data.fees_data.transactions_list) == 1
        assert coin_data.fees_data.total_quantity == Decimal(-1)
        assert coin_data.fees_data.transactions_list[0].cost_per_unit == Decimal(10)

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

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_GAINS = False
        self.api.process_coin_data('BTC')

        coin_earn = self.api.get_coin_data('BTC').coin_earn

        assert coin_earn.total_earn_quantity == Decimal(4)
        assert coin_earn.current_conversion_rate == Decimal(50)
        assert coin_earn.total_current_value == Decimal(200)

    def test_coin_data_earnings_amortized(self):
        time_start = datetime.now() - timedelta(days=10)
        time_1 = time_start + timedelta(days=1)
        time_2 = time_start + timedelta(days=2)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', time_1, Decimal(20))
        self.external_api.add_fake_cache_data('BTCEUR', time_2, Decimal(30))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_pos_int_proto(Decimal(1), time_start),
                      self._create_BTC_pos_int_proto(Decimal(2), time_1),
                      self._create_BTC_pos_int_proto(Decimal(1), time_2),
                      self._create_BTC_sell_proto(Decimal(-2), time_2)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.process_coin_data('BTC')

        coin_earn = self.api.get_coin_data('BTC').coin_earn

        assert len(coin_earn.amortized_quantities) == 1
        assert coin_earn.current_quantity == Decimal(2)
        assert coin_earn.current_value == Decimal(100)
        assert coin_earn.realized_gains == Decimal(60)

    def test_coin_data_gains_single_buy(self):
        time_start = datetime.now() - timedelta(days=10)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC', True)

        assert len(coin_data.buy_transactions_data) == 1
        buy_transaction = coin_data.buy_transactions_data[0]
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(10)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(500)
        assert buy_transaction.change_value == Decimal(400)
        assert buy_transaction.change_percentage == Decimal(4)
        assert buy_transaction.change_percentage_string == "400.00%"
        assert buy_transaction.current_cost == Decimal(100)
        assert buy_transaction.current_quantity == Decimal(10)

        assert coin_data.current_average_cost == Decimal(10)

        assert len(buy_transaction.amortized) == 0
        assert buy_transaction.total_amortized == Decimal(0)
        assert buy_transaction.total_amortized_value == Decimal(0)
        assert buy_transaction.unrealized_gains == Decimal(400)
        assert buy_transaction.unrealized_gains_change_percentage == 4
        assert buy_transaction.unrealized_gains_change_percentage_str == "400.00%"
        assert buy_transaction.realized_gains == Decimal(0)
        assert buy_transaction.realized_gains_change_percentage == 0.0
        assert buy_transaction.realized_gains_change_percentage_str == "0.00%"

    def test_coin_data_gains_single_buy_single_sell(self):
        time_start = datetime.now() - timedelta(days=10)
        time_1 = time_start + timedelta(days=1)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', time_1, Decimal(20))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_sell_proto(Decimal(-5), time_1)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC', full=True)

        assert len(coin_data.buy_transactions_data) == 1
        buy_transaction = coin_data.buy_transactions_data[0]
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(10)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(500)
        assert buy_transaction.change_value == Decimal(400)
        assert buy_transaction.change_percentage == Decimal(4)
        assert buy_transaction.change_percentage_string == "400.00%"
        assert buy_transaction.current_quantity == Decimal(5)
        assert buy_transaction.current_cost == Decimal(50)

        assert coin_data.current_average_cost == Decimal(10)

        assert len(buy_transaction.amortized) == 1
        amortized = buy_transaction.amortized[0]
        assert amortized.quantity == Decimal(5)
        assert amortized.total_value == Decimal(100)
        assert buy_transaction.total_amortized == Decimal(5)
        assert buy_transaction.total_amortized_value == Decimal(100)
        assert buy_transaction.unrealized_gains == Decimal(200)
        assert buy_transaction.unrealized_gains_change_percentage == 4.0
        assert buy_transaction.unrealized_gains_change_percentage_str == "400.00%"
        assert buy_transaction.realized_gains == Decimal(50)
        self.assertAlmostEqual(buy_transaction.realized_gains_change_percentage, 2.0)
        assert buy_transaction.realized_gains_change_percentage_str == "200.00%"

    def test_coin_data_gains_single_buy_multiple_sell(self):
        time_start = datetime.now() - timedelta(days=10)
        time_1 = time_start + timedelta(days=1)
        time_2 = time_start + timedelta(days=2)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', time_1, Decimal(20))
        self.external_api.add_fake_cache_data('BTCEUR', time_2, Decimal(30))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))
        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_sell_proto(Decimal(-5), time_1),
                      self._create_BTC_sell_proto(Decimal(-2), time_2)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC', full=True)

        assert len(coin_data.buy_transactions_data) == 1
        buy_transaction = coin_data.buy_transactions_data[0]
        assert buy_transaction.current_quantity == Decimal(3)
        assert buy_transaction.current_cost == Decimal(30)
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(10)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(500)
        assert buy_transaction.change_value == Decimal(400)
        assert buy_transaction.change_percentage == Decimal(4)
        assert buy_transaction.change_percentage_string == "400.00%"

        assert coin_data.current_average_cost == Decimal(10)

        assert len(buy_transaction.amortized) == 2
        amortized = buy_transaction.amortized[0]
        assert amortized.quantity == Decimal(5)
        assert amortized.total_value == Decimal(100)
        amortized = buy_transaction.amortized[1]
        assert amortized.quantity == Decimal(2)
        assert amortized.total_value == Decimal(60)
        assert buy_transaction.total_amortized == Decimal(7)
        assert buy_transaction.total_amortized_value == Decimal(160)
        assert buy_transaction.unrealized_gains == Decimal(120)
        assert buy_transaction.unrealized_gains_change_percentage == 4.0
        assert buy_transaction.unrealized_gains_change_percentage_str == "400.00%"
        assert buy_transaction.realized_gains == Decimal(90)
        self.assertAlmostEqual(buy_transaction.realized_gains_change_percentage, 2.285714285714286)
        assert buy_transaction.realized_gains_change_percentage_str == "228.57%"

    def test_coin_data_gains_multiple_buy_single_sell_smaller(self):
        time_start = datetime.now() - timedelta(days=10)
        time_1 = time_start + timedelta(days=1)
        time_2 = time_start + timedelta(days=2)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', time_1, Decimal(20))
        self.external_api.add_fake_cache_data('BTCEUR', time_2, Decimal(30))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))

        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_buy_proto(Decimal(5), time_1),
                      self._create_BTC_sell_proto(Decimal(-2), time_2)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC')

        assert len(coin_data.buy_transactions_data) == 2
        buy_transaction = coin_data.buy_transactions_data[0]
        assert buy_transaction.current_quantity == Decimal(8)
        assert buy_transaction.current_cost == Decimal(80)
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(10)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(500)
        assert buy_transaction.change_value == Decimal(400)
        assert buy_transaction.change_percentage == Decimal(4)
        assert buy_transaction.change_percentage_string == "400.00%"

        self.assertAlmostEqual(float(coin_data.current_average_cost), 13.8461538)

    def test_coin_data_gains_multiple_buy_single_sell_bigger(self):
        time_start = datetime.now() - timedelta(days=10)
        time_1 = time_start + timedelta(days=1)
        time_2 = time_start + timedelta(days=2)
        self.external_api.add_fake_cache_data('BTCEUR', time_start, Decimal(10))
        self.external_api.add_fake_cache_data('BTCEUR', time_1, Decimal(20))
        self.external_api.add_fake_cache_data('BTCEUR', time_2, Decimal(30))
        self.external_api.add_fake_cache_data('BTCEUR', datetime.now(), Decimal(50))

        proto_list = [self._create_BTC_buy_proto(Decimal(10), time_start),
                      self._create_BTC_buy_proto(Decimal(5), time_1),
                      self._create_BTC_sell_proto(Decimal(-12), time_2)]

        valid_transactions = self.validator.validate_and_parse_transactions(proto_list)
        self.api.add_transaction(valid_transactions)
        self.api.COMPUTE_SPOT_QUANTITIES = False
        self.api.COMPUTE_EARN_QUANTITIES = False
        self.api.COMPUTE_FEES_QUANTITIES = False
        self.api.COMPUTE_EARNINGS = False
        self.api.process_coin_data('BTC')

        coin_data = self.api.get_coin_data('BTC', full=True)

        assert len(coin_data.buy_transactions_data) == 2
        buy_transaction = coin_data.buy_transactions_data[0]
        assert buy_transaction.current_quantity == Decimal(0)
        assert buy_transaction.current_cost == Decimal(0)
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(10)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(500)
        assert buy_transaction.change_value == Decimal(400)
        assert buy_transaction.change_percentage == Decimal(4)
        assert buy_transaction.change_percentage_string == "400.00%"

        assert len(buy_transaction.amortized) == 1
        amortized = buy_transaction.amortized[0]
        assert amortized.quantity == Decimal(10)
        assert amortized.total_value == Decimal(300)
        assert buy_transaction.total_amortized == Decimal(10)
        assert buy_transaction.total_amortized_value == Decimal(300)
        assert buy_transaction.unrealized_gains == Decimal(0)
        assert buy_transaction.unrealized_gains_change_percentage == 0.0
        assert buy_transaction.unrealized_gains_change_percentage_str == "0.00%"
        assert buy_transaction.realized_gains == Decimal(200)
        self.assertAlmostEqual(buy_transaction.realized_gains_change_percentage, 3.0)
        assert buy_transaction.realized_gains_change_percentage_str == "300.00%"

        buy_transaction = coin_data.buy_transactions_data[1]
        assert buy_transaction.current_quantity == Decimal(3)
        assert buy_transaction.current_cost == Decimal(60)
        assert buy_transaction.cost == Decimal(100)
        assert buy_transaction.cost_per_unit == Decimal(20)
        assert buy_transaction.current_value_per_unit == Decimal(50)
        assert buy_transaction.current_value == Decimal(250)
        assert buy_transaction.change_value == Decimal(150)
        assert buy_transaction.change_percentage == Decimal(1.5)
        assert buy_transaction.change_percentage_string == "150.00%"

        assert len(buy_transaction.amortized) == 1
        amortized = buy_transaction.amortized[0]
        assert amortized.quantity == Decimal(2)
        assert amortized.total_value == Decimal(60)
        assert buy_transaction.total_amortized == Decimal(2)
        assert buy_transaction.total_amortized_value == Decimal(60)
        assert buy_transaction.unrealized_gains == Decimal(90)
        assert buy_transaction.unrealized_gains_change_percentage == 1.5
        assert buy_transaction.unrealized_gains_change_percentage_str == "150.00%"
        assert buy_transaction.realized_gains == Decimal(20)
        self.assertAlmostEqual(buy_transaction.realized_gains_change_percentage, 1.5)
        assert buy_transaction.realized_gains_change_percentage_str == "150.00%"

        self.assertAlmostEqual(float(coin_data.current_average_cost), 20)
