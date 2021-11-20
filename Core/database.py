from typing import Dict, List, Optional, Iterable, Set, Callable
from decimal import Decimal
from datetime import datetime

from .Dataclasses import Coin, Transaction, CoinData, CoinEarn, BuyTransactionData, Amortization, FeeData
from .CoinAPIExternal import CoinAPI, APIBase
from .Dataclasses import ProtoTransaction

SPOT_OPERATIONS = (Transaction.TransactionType.SAVING_REDEMPTION,
                   Transaction.TransactionType.SAVING_INTEREST,
                   Transaction.TransactionType.POS_REDEMPTION,
                   Transaction.TransactionType.POS_INTEREST,
                   Transaction.TransactionType.BUY,
                   Transaction.TransactionType.DEPOSIT,
                   Transaction.TransactionType.FEE,
                   Transaction.TransactionType.SELL,
                   Transaction.TransactionType.SAVING_PURCHASE,
                   Transaction.TransactionType.POS_PURCHASE,
                   Transaction.TransactionType.LIQUID_SWAP_ADD,
                   Transaction.TransactionType.LIQUID_SWAP_REDEMPTION)

IN_SPOT_OPERATIONS = (Transaction.TransactionType.SAVING_REDEMPTION,
                      Transaction.TransactionType.SAVING_INTEREST,
                      Transaction.TransactionType.POS_REDEMPTION,
                      Transaction.TransactionType.POS_INTEREST,
                      Transaction.TransactionType.BUY,
                      Transaction.TransactionType.DEPOSIT,
                      Transaction.TransactionType.LIQUID_SWAP_REDEMPTION)
OUT_SPOT_OPERATIONS = (Transaction.TransactionType.FEE,
                       Transaction.TransactionType.SELL,
                       Transaction.TransactionType.SAVING_PURCHASE,
                       Transaction.TransactionType.POS_PURCHASE,
                       Transaction.TransactionType.LIQUID_SWAP_ADD)

EARN_OPERATIONS = (Transaction.TransactionType.SAVING_PURCHASE, Transaction.TransactionType.POS_PURCHASE,
                   Transaction.TransactionType.LIQUID_SWAP_ADD,
                   Transaction.TransactionType.SAVING_REDEMPTION, Transaction.TransactionType.POS_REDEMPTION,
                   Transaction.TransactionType.LIQUID_SWAP_REDEMPTION)


class _BuyTransaction:

    def __init__(self, transaction: Transaction, cost_per_unit: Decimal, current_value_callback: callable):
        self._current_value_per_unit_callback = current_value_callback
        self.transaction = transaction
        self.cost_per_unit = cost_per_unit
        self.cost = self.transaction.quantity * self.cost_per_unit
        self.amortized_quantities: List[Amortization] = []

    @property
    def current_value_per_unit(self):
        return self._current_value_per_unit_callback()

    @property
    def current_value(self):
        return self.transaction.quantity * self.current_value_per_unit

    @property
    def change_value(self):
        return self.current_value - self.cost

    @property
    def change_percentage(self):
        return float(self.change_value / self.cost)

    @property
    def change_percentage_string(self):
        return "{0:.2%}".format(self.change_percentage)

    @property
    def spot_quantity(self):
        # Change name because this is not spot it is all quantity
        if not self.amortized_quantities:
            return self.transaction.quantity
        return self.transaction.quantity - sum(x.quantity for x in self.amortized_quantities)

    @property
    def current_cost(self):
        return self.spot_quantity * self.cost_per_unit

    @property
    def total_amortized(self):
        if not self.amortized_quantities:
            return Decimal(0)
        return sum(x.quantity for x in self.amortized_quantities)

    @property
    def total_amortized_value(self):
        if not self.amortized_quantities:
            return Decimal(0)
        return sum(x.total_value for x in self.amortized_quantities)

    @property
    def unrealized_total_value(self):
        current_quantity = self.spot_quantity
        if not current_quantity > 0:
            return Decimal(0)
        return current_quantity * self.current_value_per_unit

    @property
    def unrealized_gains(self):
        current_quantity = self.spot_quantity
        if not current_quantity > 0:
            return Decimal(0)

        return (current_quantity * self.current_value_per_unit) - (current_quantity * self.cost_per_unit)

    @property
    def unrealized_gains_change_percentage(self):
        current_quantity = self.spot_quantity
        if not current_quantity > 0:
            return 0.0

        return float(self.unrealized_gains / (current_quantity * self.cost_per_unit))

    @property
    def unrealized_gains_change_percentage_str(self):
        return "{0:.2%}".format(self.unrealized_gains_change_percentage)

    @property
    def realized_gains(self):
        if not self.amortized_quantities:
            return Decimal(0)
        return self.total_amortized_value - (self.total_amortized * self.cost_per_unit)

    @property
    def realized_gains_change_percentage(self):
        if not self.amortized_quantities:
            return 0.0

        return float(self.total_amortized_value / (self.total_amortized * self.cost_per_unit))

    @property
    def realized_gains_change_percentage_str(self):
        return "{0:.2%}".format(self.realized_gains_change_percentage)

    def add_amortized(self, quantity: Decimal, total_value: Decimal):
        self.amortized_quantities.append(Amortization(quantity, total_value))

    def get_frozen_data(self, full=True):
        return BuyTransactionData(transaction=self.transaction,
                                  cost_per_unit=self.cost_per_unit,
                                  cost=self.cost,
                                  current_value_per_unit=self.current_value_per_unit,
                                  current_value=self.current_value,
                                  change_value=self.change_value,
                                  change_percentage=self.change_percentage,
                                  change_percentage_string=self.change_percentage_string,
                                  current_quantity=self.spot_quantity,
                                  current_cost=self.current_cost,
                                  total_amortized=self.total_amortized,
                                  total_amortized_value=self.total_amortized_value,
                                  unrealized_total_value=self.unrealized_total_value,
                                  unrealized_gains=self.unrealized_gains,
                                  unrealized_gains_change_percentage=self.unrealized_gains_change_percentage,
                                  unrealized_gains_change_percentage_str=self.unrealized_gains_change_percentage_str,
                                  realized_gains=self.realized_gains,
                                  realized_gains_change_percentage=self.realized_gains_change_percentage,
                                  realized_gains_change_percentage_str=self.realized_gains_change_percentage_str,
                                  amortized=self.amortized_quantities if full else None)


class _CoinData:

    def __init__(self, coin: Coin, current_value_per_unit_callback: Callable[['_CoinData'], Decimal]):
        self._coin: Coin = coin

        self.spot_quantity: Optional[Decimal] = None
        self.earn_quantity: Optional[Decimal] = None

        self._transactions_groups: Dict[Transaction.TransactionType, List[Transaction]] = self._group_transactions()

        self.buy_transactions: List[_BuyTransaction] = []
        self.coin_earn: Optional[CoinEarn] = None
        self.fees_data: FeeData = FeeData()

        self.current_value_per_unit_callback = current_value_per_unit_callback

    def _group_transactions(self):
        transactions_groups = {x: [] for x in Transaction.TransactionType}

        for trans in self._coin.transactions:
            transactions_groups[trans.operation_type].append(trans)
        return transactions_groups

    def get_coin_tick(self):
        return self._coin.coin_info.tick

    def get_current_value_per_unit(self) -> Decimal:
        return self.current_value_per_unit_callback(self)

    def get_transactions(self, type_filter: Optional[Iterable[Transaction.TransactionType]] = None):
        if type_filter is None:
            return self._coin.transactions
        else:
            trans_list = []
            for current_type in type_filter:
                trans_list.extend(self._transactions_groups[current_type])
            return trans_list

    def get_buy_sell_transactions(self):
        buy_sell_trans = self.get_transactions((Transaction.TransactionType.BUY, Transaction.TransactionType.SELL))
        buy_sell_trans.sort(key=lambda x: x.UTC_Time)
        return buy_sell_trans

    def create_coin_earn(self):
        return CoinEarn(self._coin, Decimal(0))

    def get_frozen_coin_data(self, full=True) -> CoinData:
        return CoinData(coin=self._coin, spot_quantity=self.spot_quantity, earn_quantity=self.earn_quantity,
                        current_value_per_unit=self.get_current_value_per_unit(),
                        current_average_cost=self._get_current_average_cost(), total_costs=self._get_total_costs(),
                        total_current_cost=self._get_total_current_cost(),
                        total_unrealized_gains=self._get_total_unrealized_gains(),
                        current_total_value=self._get_current_total_value(),
                        total_realized_gains=self._get_total_realized_gains(),
                        total_realized_value=self._get_total_realized_value(),
                        buy_transactions_data=self._freeze_buy_transactions(full),
                        fees_data=self.fees_data,
                        coin_earn=self.coin_earn)

    def _get_current_average_cost(self):
        if not self.buy_transactions:
            return Decimal(0.0)
        return self._get_total_current_cost() / sum(trans.spot_quantity for trans in self.buy_transactions)

    def _get_total_costs(self):
        return sum(trans.cost for trans in self.buy_transactions)

    def _get_total_current_cost(self):
        return sum(trans.current_cost for trans in self.buy_transactions)

    def _get_total_unrealized_gains(self):
        return sum(trans.unrealized_gains for trans in self.buy_transactions)

    def _get_current_total_value(self):
        return sum(trans.unrealized_total_value for trans in self.buy_transactions)

    def _get_total_realized_gains(self):
        return sum(trans.realized_gains for trans in self.buy_transactions)

    def _get_total_realized_value(self):
        return sum(trans.total_amortized_value for trans in self.buy_transactions)

    def _freeze_buy_transactions(self, full):
        return [trans.get_frozen_data(full) for trans in self.buy_transactions]


class DataBase:
    name: str
    holdings: Dict[str, Coin]
    holdings_data: Dict[str, _CoinData]
    transactions: Dict[str, Transaction]


class TransactionValidator:

    def __init__(self, database: 'DataBaseAPI', duplicates_cache_path: str = None):
        self._duplicate_ids = self._acknowledge_duplicates(duplicates_cache_path)
        self._database_api = database

    @staticmethod
    def _acknowledge_duplicates(path: str) -> Set[str]:
        if path is None:
            return set()

        with open(path, 'r') as f:
            return set(line.strip() for line in f.readlines())

    def validate_and_parse_transactions(self, list_transactions: List[ProtoTransaction]) -> List[Transaction]:
        new_transactions = []
        for proto_transaction in list_transactions:
            transaction = self._parse_proto_transaction(proto_transaction)
            self._validate_quantity(transaction)
            new_transactions.append(transaction)

        valid_transactions = self._clean_duplicates_imports(new_transactions)
        self._check_duplicate_in_database(valid_transactions)

        return valid_transactions

    @staticmethod
    def _validate_quantity(transaction: Transaction):
        if transaction.operation_type in IN_SPOT_OPERATIONS:
            assert transaction.quantity >= 0, "Enter operations must be positive"
        if transaction.operation_type in OUT_SPOT_OPERATIONS:
            assert transaction.quantity <= 0, "Exit operations must be negative"

    def _check_duplicate_in_database(self, list_transactions: List[Transaction]):
        for transaction in list_transactions:
            self._database_api.exists_transaction(transaction)

    def _clean_duplicates_imports(self, list_transactions: List[Transaction]) -> List[Transaction]:
        seen = set()

        valid_transactions = []
        for transaction in list_transactions:
            if transaction.id in seen:
                if transaction.id in self._duplicate_ids:
                    continue
                self._raise_duplicated(list_transactions, transaction)

            seen.add(transaction.id)
            valid_transactions.append(transaction)

        return valid_transactions

    @staticmethod
    def _raise_duplicated(list_transactions: List[Transaction], transaction: Transaction):
        found_duplicates = []
        for duplicated_candidate in list_transactions:
            if duplicated_candidate == transaction:
                found_duplicates.append(duplicated_candidate)
        raise ValueError(f"Duplicated transactions in the import list\n " + '\n'.join(str(x) for x in found_duplicates))

    def _parse_proto_transaction(self, proto: ProtoTransaction) -> Transaction:
        coin = self._get_database_coin(proto)

        if proto.operation_type is Transaction.TransactionType.BUY and proto.value < 0:
            operation_type = Transaction.TransactionType.SELL
        elif proto.operation_type is Transaction.TransactionType.SELL and proto.value > 0:
            operation_type = Transaction.TransactionType.BUY
        else:
            operation_type = proto.operation_type

        return Transaction(proto.value, coin, operation_type, proto.UTC_Time, proto.account)

    def _get_database_coin(self, proto: ProtoTransaction) -> Coin:
        try:
            return self._database_api.get_coin(proto.coin_name)
        except KeyError:
            return self._database_api.add_coin(proto.coin_name)


class CoinDataProcessor:

    def __init__(self, conversion_callback):
        self._conversion_callback = conversion_callback

    @staticmethod
    def compute_spot_quantities(coin_data: _CoinData):
        value = Decimal(0)
        for trans in coin_data.get_transactions():
            value += trans.quantity
        coin_data.spot_quantity = value

    @staticmethod
    def compute_earn_quantities(coin_data: _CoinData):
        value = Decimal(0)
        for trans in coin_data.get_transactions():
            if trans.operation_type in EARN_OPERATIONS:
                value -= trans.quantity
        coin_data.earn_quantity = value

    def compute_fees_quantities(self, coin_data: _CoinData):
        for trans in coin_data.get_transactions((Transaction.TransactionType.FEE,)):
            cost_per_unit = self._conversion_callback(coin_data.get_coin_tick(), trans.UTC_Time)
            coin_data.fees_data.create_fee_transaction(trans, cost_per_unit)

    @staticmethod
    def compute_earnings(coin_data: _CoinData):
        list_trans_to_process = coin_data.get_transactions((Transaction.TransactionType.POS_INTEREST,
                                                            Transaction.TransactionType.SAVING_INTEREST))
        coin_earn = coin_data.create_coin_earn()
        for trans in list_trans_to_process:
            coin_earn.total_earn_quantity += trans.quantity

        coin_earn.update_current_conversion_rate(coin_data.get_current_value_per_unit())
        coin_data.coin_earn = coin_earn

    def compute_gains(self, coin_data: _CoinData):
        buy_transactions: List[_BuyTransaction] = []

        list_trans_to_process = coin_data.get_buy_sell_transactions()

        for trans in list_trans_to_process:
            if trans.operation_type is Transaction.TransactionType.BUY:
                buy_transactions.append(self._create_buy_transaction(coin_data, trans))
            elif trans.operation_type is Transaction.TransactionType.SELL:
                sell_quantity = trans.quantity
                # We want to be positive
                if sell_quantity < 0:
                    sell_quantity *= -1

                for buy_trans in buy_transactions:
                    current_quantity = buy_trans.spot_quantity
                    if current_quantity:
                        if sell_quantity <= current_quantity:
                            self._add_amortization(buy_trans, sell_quantity, trans)
                        else:
                            self._add_amortization(buy_trans, current_quantity, trans)
                        sell_quantity -= current_quantity
                    if sell_quantity <= 0:
                        break
                else:
                    self._amortize_earn_coins(coin_data, sell_quantity, trans)

        if buy_transactions:
            coin_data.buy_transactions = buy_transactions

    def _amortize_earn_coins(self, coin_data: _CoinData, sell_quantity: Decimal, trans: Transaction):
        if not coin_data.coin_earn:
            raise ValueError(f"Not earn data for coin {coin_data.get_coin_tick()}")
        if sell_quantity > coin_data.coin_earn.current_quantity:
            raise ValueError(f"Not enough earn coins {coin_data.get_coin_tick()} to sell")

        sell_value = self._conversion_callback(trans.coin.coin_info.tick, trans.UTC_Time)
        coin_data.coin_earn.add_amortized(sell_quantity, sell_value * sell_quantity)

    def _add_amortization(self, buy_transaction: _BuyTransaction, sell_quantity: Decimal, trans: Transaction):
        sell_value = self._conversion_callback(trans.coin.coin_info.tick, trans.UTC_Time)
        buy_transaction.add_amortized(sell_quantity, sell_value * sell_quantity)

    def _create_buy_transaction(self, coin_data: _CoinData, trans: Transaction):
        cost = self._conversion_callback(trans.coin.coin_info.tick, trans.UTC_Time)
        return _BuyTransaction(transaction=trans, cost_per_unit=cost,
                               current_value_callback=coin_data.get_current_value_per_unit)


class DataBaseAPI:
    class Precision:
        M1 = '1Minute'
        M15 = '15Minute'
        M30 = '30Minute'
        H1 = '1Hour'

    COMPUTE_SPOT_QUANTITIES = True
    COMPUTE_EARN_QUANTITIES = True
    COMPUTE_FEES_QUANTITIES = True
    COMPUTE_EARNINGS = True
    COMPUTE_GAINS = True

    def __init__(self, database: DataBase, external_api: APIBase, return_fiat='EUR',
                 now_precision: Precision = Precision.M15):
        self._database = database
        self._external_api = external_api
        self._return_fiat = return_fiat
        self._now_precision = now_precision

        self._coin_data_processor = CoinDataProcessor(self._get_conversion_rate_callback)

    @property
    def active_processes(self) -> List[Callable[[_CoinData], None]]:
        active = []
        if self.COMPUTE_SPOT_QUANTITIES:
            active.append(self._coin_data_processor.compute_spot_quantities)
        if self.COMPUTE_EARN_QUANTITIES:
            active.append(self._coin_data_processor.compute_earn_quantities)
        if self.COMPUTE_FEES_QUANTITIES:
            active.append(self._coin_data_processor.compute_fees_quantities)
        if self.COMPUTE_EARNINGS:
            active.append(self._coin_data_processor.compute_earnings)
        if self.COMPUTE_GAINS:
            active.append(self._coin_data_processor.compute_gains)
        return active

    @staticmethod
    def create_new_database(name='default'):
        db = DataBase()
        db.name = name
        db.holdings = {}
        db.transactions = {}
        db.holdings_data = {}
        return db

    def print_coin_data(self, coin_tick):
        self.get_coin_data(coin_tick).print_status()

    def add_coin(self, coin_name: str) -> Coin:
        if coin_name in self._database.holdings:
            raise KeyError(f"The coin {coin_name} already exist in the database")

        try:
            coin_info = CoinAPI.get_coin_info(coin_name)
        except KeyError:
            raise KeyError(f"The coin {coin_name} doesn't exist")

        new_coin = Coin(coin_info, [])

        self._database.holdings[coin_name] = new_coin
        return new_coin

    def remove_coin(self, coin_name: str, force: bool = False):
        try:
            coin = self._database.holdings[coin_name]
        except KeyError:
            raise KeyError(f"The coin {coin_name} doesn't exist in the database")

        if force is False and len(coin.transactions) > 1:
            raise ValueError(f"The coin {coin_name} has transactions is not safe to delete it")

        if len(coin.transactions) > 1:
            for transaction in coin.transactions:
                del self._database.transactions[transaction.id]

        del self._database.holdings[coin_name]
        try:
            del self._database.holdings_data[coin_name]
        except KeyError:
            pass

    def get_coin_list(self) -> List[str]:
        return list(self._database.holdings.keys())

    def get_coin(self, coin_name: str) -> Coin:
        return self._database.holdings[coin_name]

    def get_coin_data(self, coin_name: str, full: bool = False) -> CoinData:
        return self._database.holdings_data[coin_name].get_frozen_coin_data(full)

    def _get_coin_data(self, coin_name: str) -> _CoinData:
        return self._database.holdings_data[coin_name]

    def add_transaction(self, transaction: [Transaction, List[Transaction]]):
        if isinstance(transaction, list):
            for element in transaction:
                self.add_transaction(element)

            return None

        if transaction.coin.coin_info.tick not in self._database.holdings:
            coin = self.add_coin(transaction.coin.coin_info.tick)
        else:
            coin = self.get_coin(transaction.coin.coin_info.tick)

        coin.transactions.append(transaction)
        self._database.transactions[transaction.id] = transaction

    def exists_transaction(self, transaction: Transaction):
        if transaction.id in self._database.transactions:
            raise ValueError(f"Transaction {transaction} not valid, it is duplicated in the database")

    def _get_now_time(self) -> datetime:
        now_full = datetime.now()
        if self._now_precision == self.Precision.H1:
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour)
        elif self._now_precision == self.Precision.M30:
            minutes = 30 if now_full.minute >= 30 else 0
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, minutes)
        elif self._now_precision == self.Precision.M15:
            minutes = int(now_full.minute / 15) * 15
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, minutes)
        elif self._now_precision == self.Precision.M1:
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, now_full.minute)

    def process_coin_data(self, coin_tick: Optional[str] = None):
        if coin_tick is None:
            self.process_all_coins_data()
        else:
            coin_data = self._get_or_create_coin_data(coin_tick)
            for process in self.active_processes:
                process(coin_data)

    def process_all_coins_data(self):
        coins_list = list(self._database.holdings.keys())
        number_of_coins = len(coins_list)
        for i, coin_tick in enumerate(coins_list):
            print(f"Processing coin {coin_tick}, {i}/{number_of_coins}")
            if coin_tick in ('EUR', 'BUSD', 'USDT'):
                continue
            self.process_coin_data(coin_tick)

    def _get_or_create_coin_data(self, coin_tick: str):
        try:
            coin_data = self._get_coin_data(coin_tick)
        except KeyError:
            coin_data = _CoinData(self.get_coin(coin_tick), self._get_conversion_rate_now_callback)
            self._database.holdings_data[coin_tick] = coin_data
        return coin_data

    def _get_conversion_rate_callback(self, coin_tick: str, date: datetime):
        return self._external_api.get_conversion_rate(coin_tick, self._return_fiat, date)

    def _get_conversion_rate_now_callback(self, coin_data: _CoinData):
        return self._external_api.get_conversion_rate(coin_data.get_coin_tick(), self._return_fiat,
                                                      self._get_now_time())
