from typing import Dict, List, Optional, Iterable, Tuple
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field

from .Dataclasses import Coin, Transaction
from .CoinAPIExternal import CoinAPI, APIBase
from .Dataclasses import ProtoTransaction

IN_STOCK_OPERATIONS = (Transaction.TransactionType.SAVING_REDEMPTION,
                       Transaction.TransactionType.SAVING_INTEREST,
                       Transaction.TransactionType.POS_REDEMPTION,
                       Transaction.TransactionType.POS_INTEREST,
                       Transaction.TransactionType.BUY,
                       Transaction.TransactionType.DEPOSIT,
                       Transaction.TransactionType.FEE,
                       Transaction.TransactionType.SELL,
                       Transaction.TransactionType.SAVING_PURCHASE,
                       Transaction.TransactionType.POS_PURCHASE)

IN_EARN_OPERATIONS = (Transaction.TransactionType.SAVING_PURCHASE, Transaction.TransactionType.POS_PURCHASE,
                      Transaction.TransactionType.SAVING_REDEMPTION, Transaction.TransactionType.POS_REDEMPTION)


class CoinData:

    def __init__(self, coin: Coin):
        self.coin = coin

        self._stock_quantity = None
        self._earn_quantity = None

        self.current_value_per_unit = None

        self._transactions_groups: Dict[Transaction.TransactionType, List[Transaction]] = self._group_transactions()
        self._fees_transactions: List[FeeData] = []
        self._buy_transactions_data: List[BuyTransactionData] = []
        self._coin_earn = None

        self.current_average_cost = Decimal(0)

    def _group_transactions(self):
        transactions_groups = {x: [] for x in Transaction.TransactionType}

        for trans in self.coin.transactions:
            transactions_groups[trans.operation_type].append(trans)
        return transactions_groups

    def get_transactions(self, type_filter: Optional[Iterable[Transaction.TransactionType]] = None):
        if type_filter is None:
            return self.coin.transactions
        else:
            trans_list = []
            for current_type in type_filter:
                trans_list.extend(self._transactions_groups[current_type])
            return trans_list

    def get_buy_sell_transactions(self):
        buy_sell_trans = self.get_transactions((Transaction.TransactionType.BUY, Transaction.TransactionType.SELL))
        buy_sell_trans.sort(key=lambda x: x.UTC_Time)
        return buy_sell_trans

    def update_current_value(self, value):
        self.current_value_per_unit = value

    def _get_total_unrealized_gains(self):
        return sum(trans.unrealized_gains for trans in self._buy_transactions_data)

    def _get_total_realized_gains(self):
        return sum(trans.realized_gains for trans in self._buy_transactions_data)

    def print_status(self):
        print(f"Coin info: {self.coin}")
        if self.current_value_per_unit:
            print(f"Current quantity in stock: {self._stock_quantity}; "
                  f"Current value: {self._stock_quantity * self.current_value_per_unit}")
            print(f"Current quantity in earn: {self._earn_quantity}; "
                  f"Current value: {self._earn_quantity * self.current_value_per_unit}")
        else:
            print(f"Current quantity in stock: {self._stock_quantity}; Current value: Not available")
            print(f"Current quantity in earn: {self._earn_quantity}; Current value: Not available")
        total_cost_of_fees = Decimal(0)
        for fee in self._fees_transactions:
            total_cost_of_fees += fee.cost
        print(f"Total quantity spent in fees: {total_cost_of_fees}€")
        print(f"Total value earn:")
        print(f"{self._coin_earn}")
        print(f"Current average cost: {self.current_average_cost}")

        print(f"Buy transactions cost and gains")
        for buy_trans in self._buy_transactions_data:
            print(buy_trans)

        print(f"Total unrealized gains: {self._get_total_unrealized_gains()}")
        print(f"Total realized gains: {self._get_total_realized_gains()}")


@dataclass
class TransactionData:
    transaction: Transaction
    cost_per_unit: Decimal
    current_value_per_unit: Decimal

    cost: Decimal = field(init=False)
    current_value: Decimal = field(init=False)
    change: float = field(init=False)
    change_value: Decimal = field(init=False)
    change_percentage: str = field(init=False)

    def __post_init__(self):
        self.cost = self.transaction.quantity * self.cost_per_unit
        self.current_value = self.transaction.quantity * self.current_value_per_unit
        self.change_value = self.current_value - self.cost
        self.change = float(self.change_value / self.cost)
        self.change_percentage = "{0:.2%}".format(self.change)


@dataclass
class BuyTransactionData(TransactionData):
    amortized_quantities: List[Tuple[Decimal, Decimal]] = field(init=False)

    def __post_init__(self):
        self.amortized_quantities = []
        super().__post_init__()

    def get_spot_quantity(self):
        if not self.amortized_quantities:
            return self.transaction.quantity
        return self.transaction.quantity - sum(x[0] for x in self.amortized_quantities)

    def get_current_cost(self):
        return self.get_spot_quantity() * self.cost_per_unit

    @property
    def unrealized_gains(self):
        current_quantity = self.get_spot_quantity()
        if not current_quantity > 0:
            return Decimal(0)

        return (current_quantity * self.current_value_per_unit) - (current_quantity * self.cost_per_unit)

    @property
    def realized_gains(self):
        if not self.amortized_quantities:
            return Decimal(0)
        total_amortized = sum(x[0] for x in self.amortized_quantities)
        return sum(x[1] for x in self.amortized_quantities) - (total_amortized * self.cost_per_unit)

    def add_amortized(self, quantity: Decimal, total_value: Decimal):
        self.amortized_quantities.append((quantity, total_value))


@dataclass
class FeeData:
    transaction: Transaction
    cost_per_unit: Decimal

    cost: Decimal = field(init=False)

    def __post_init__(self):
        self.cost = self.transaction.quantity * self.cost_per_unit


@dataclass
class CoinEarn:
    coin: Coin
    quantity: Decimal

    current_value: Decimal = field(init=False)
    current_conversion_rate: Decimal = field(init=False)

    def compute_current_value(self, conversion_rate):
        self.current_conversion_rate = conversion_rate
        self.current_value = self.current_conversion_rate * self.quantity


class DataBase:
    name: str
    holdings: Dict[str, Coin]
    holdings_data: Dict[str, CoinData]
    transactions: Dict[str, Transaction]


class NowPrecision:
    M1 = '1Minute'
    M15 = '15Minute'
    M30 = '30Minute'
    H1 = '1Hour'


class DataBaseAPI:
    NowPrecision = NowPrecision

    def __init__(self, external_api: APIBase, database=None, return_fiat='EUR',
                 now_precission: NowPrecision = NowPrecision.M15):
        if database is None or not isinstance(database, DataBase):
            raise ValueError("A database must exist")
        self._database = database
        self._external_api = external_api
        self._duplicate_ids = set()
        self._return_fiat = return_fiat
        self._now_precision = now_precission

        self.active_processes = (self._compute_current_value_per_coin,
                                 self._compute_spot_quantities,
                                 self._compute_earn_quantities,
                                 self._compute_fees_quantities,
                                 self._compute_gains,
                                 self._compute_earnings)

    @staticmethod
    def create_new_database(name='default'):
        db = DataBase()
        db.name = name
        db.holdings = {}
        db.transactions = {}
        db.holdings_data = {}
        return db

    def print_coin_data(self, coin_tick):
        self._get_coin_data(coin_tick).print_status()

    def acknowledge_duplicates(self, path):
        with open(path, 'r') as f:
            for line in f.readlines():
                self._duplicate_ids.add(line.strip())

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

    def get_coin(self, coin_name: str) -> Coin:
        return self._database.holdings[coin_name]

    def _get_coin_data(self, coin_name: str) -> CoinData:
        return self._database.holdings_data[coin_name]

    def validate_import(self, list_transactions: List[ProtoTransaction]):
        print("Validating transactions...")
        new_transactions = []
        for proto_transaction in list_transactions:
            transaction = self._convert_transaction(proto_transaction)
            new_transactions.append(transaction)

        return self._validate_duplicates(new_transactions)

    def _validate_duplicates(self, list_transactions: List[Transaction]) -> List[Transaction]:
        seen = set()

        valid_transactions = []
        for transaction in list_transactions:
            if transaction.id in self._duplicate_ids:
                continue

            if not self._validate_transaction(transaction):
                raise ValueError(f"Transaction {transaction} not valid, it is duplicated in the database")

        for transaction in list_transactions:

            if transaction.id in seen:
                if transaction.id in self._duplicate_ids:
                    continue

                duplicates = []
                for duplicated_candidate in list_transactions:
                    if duplicated_candidate == transaction:
                        duplicates.append(duplicated_candidate)
                raise ValueError(
                    f"Duplicated transactions in the import list\n " + '\n'.join(str(x) for x in duplicates))

            seen.add(transaction.id)
            valid_transactions.append(transaction)

        return valid_transactions

    def _convert_transaction(self, proto: ProtoTransaction) -> Transaction:
        try:
            coin = self.get_coin(proto.coin_name)
        except KeyError:
            coin = self.add_coin(proto.coin_name)

        if proto.operation_type is Transaction.TransactionType.BUY and proto.value < 0:
            operation_type = Transaction.TransactionType.SELL
        else:
            operation_type = proto.operation_type

        return Transaction(proto.value, coin, operation_type, proto.UTC_Time, proto.account)

    def _validate_transaction(self, transaction: Transaction) -> bool:
        if transaction.id in self._database.transactions:
            return False
        return True

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

    def _get_now_time(self) -> datetime:
        now_full = datetime.now()
        if self._now_precision == NowPrecision.H1:
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour)
        elif self._now_precision == NowPrecision.M30:
            minutes = 30 if now_full.minute >= 30 else 0
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, minutes)
        elif self._now_precision == NowPrecision.M15:
            minutes = int(now_full.minute / 15) * 15
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, minutes)
        elif self._now_precision == NowPrecision.M1:
            return datetime(now_full.year, now_full.month, now_full.day, now_full.hour, now_full.minute)

    def _process_transactions_data(self, transactions: List[Transaction]):
        transaction_data = []
        for trans in transactions:
            cost = self._external_api.get_conversion_rate(trans.coin.coin_info.tick, self._return_fiat, trans.UTC_Time)
            current_value = self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                   self._return_fiat,
                                                                   self._get_now_time())
            new_data = TransactionData(transaction=trans,
                                       cost_per_unit=cost,
                                       current_value_per_unit=current_value)
            transaction_data.append(new_data)
        return transaction_data

    def collect_all_earn_earnings(self, transactions: List[Transaction]) -> Dict[str, CoinEarn]:
        coin_dict = {}
        for trans in transactions:
            if trans.operation_type in (trans.TransactionType.POS_INTEREST, trans.TransactionType.SAVING_INTEREST):
                coin_tick = trans.coin.coin_info.tick
                if coin_tick in coin_dict:
                    coin_dict[coin_tick].quantity += trans.quantity
                else:
                    coin_dict[coin_tick] = CoinEarn(coin=trans.coin, quantity=trans.quantity)

        for coin_tick, coin in coin_dict.items():
            conversion_rate = self._external_api.get_conversion_rate(coin_tick, self._return_fiat, self._get_now_time())
            coin.compute_current_value(conversion_rate)

        return coin_dict

    def process_coin_data(self, coin_tick: Optional[str] = None):
        if coin_tick is None:
            for coin_tick in self._database.holdings_data.keys():
                self.process_coin_data(coin_tick)
        else:
            try:
                coin_data = self._get_coin_data(coin_tick)
            except KeyError:
                coin_data = CoinData(self.get_coin(coin_tick))
                self._database.holdings_data[coin_tick] = coin_data
            for process in self.active_processes:
                process(coin_data)

    def _compute_current_value_per_coin(self, coin_data: CoinData):
        coin_data.update_current_value(self._external_api.get_conversion_rate(coin_data.coin.coin_info.tick,
                                                                              self._return_fiat,
                                                                              self._get_now_time()))

    def _compute_spot_quantities(self, coin_data: CoinData):
        value = Decimal(0)
        for trans in coin_data.coin.transactions:
            value += trans.quantity
        coin_data._stock_quantity = value

    def _compute_earn_quantities(self, coin_data: CoinData):
        value = Decimal(0)
        for trans in coin_data.coin.transactions:
            if trans.operation_type in IN_EARN_OPERATIONS:
                value -= trans.quantity
        coin_data._earn_quantity = value

    def _compute_fees_quantities(self, coin_data: CoinData):
        list_trans = []
        for trans in coin_data.get_transactions((Transaction.TransactionType.FEE,)):
            cost_per_unit = self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                   self._return_fiat,
                                                                   trans.UTC_Time)

            list_trans.append(FeeData(trans, cost_per_unit))

        coin_data._fees_transactions = list_trans

    # def _compute_buy_transactions(self, coin_data: CoinData):
    #     list_trans_to_process = coin_data.get_transactions((Transaction.TransactionType.BUY,))
    #     data_processed = self._process_transactions_data(list_trans_to_process)
    #     coin_data._buy_transactions_data = data_processed

    def _compute_earnings(self, coin_data: CoinData):
        list_trans_to_process = coin_data.get_transactions((Transaction.TransactionType.POS_INTEREST,
                                                            Transaction.TransactionType.SAVING_INTEREST))
        coin_earn = CoinEarn(coin_data.coin, Decimal(0))
        for trans in list_trans_to_process:
            coin_earn.quantity += trans.quantity

        conversion_rate = self._external_api.get_conversion_rate(coin_data.coin.coin_info.tick,
                                                                 self._return_fiat,
                                                                 self._get_now_time())
        coin_earn.compute_current_value(conversion_rate)
        coin_data._coin_earn = coin_earn

    def _compute_gains(self, coin_data: CoinData):
        buy_transactions: List[BuyTransactionData] = []

        list_trans_to_process = coin_data.get_buy_sell_transactions()

        for trans in list_trans_to_process:
            if trans.operation_type is Transaction.TransactionType.BUY:
                cost = self._external_api.get_conversion_rate(trans.coin.coin_info.tick, self._return_fiat,
                                                              trans.UTC_Time)
                current_value = self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                       self._return_fiat,
                                                                       self._get_now_time())
                new_data = BuyTransactionData(transaction=trans,
                                              cost_per_unit=cost,
                                              current_value_per_unit=current_value)
                buy_transactions.append(new_data)
            elif trans.operation_type is Transaction.TransactionType.SELL:
                sell_quantity = trans.quantity
                # We want to be positive
                if sell_quantity < 0:
                    sell_quantity *= -1

                for buy_trans in buy_transactions:
                    if sell_quantity <= 0:
                        break
                    current_quantity = buy_trans.get_spot_quantity()
                    if current_quantity:
                        if sell_quantity <= current_quantity:
                            sell_value = self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                                self._return_fiat,
                                                                                trans.UTC_Time)
                            buy_trans.add_amortized(sell_quantity, sell_value * sell_quantity)
                            break
                        else:
                            amortized_quantity = sell_quantity - current_quantity
                            sell_value = self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                                self._return_fiat,
                                                                                trans.UTC_Time)
                            buy_trans.add_amortized(amortized_quantity, sell_value * amortized_quantity)
                            sell_quantity -= amortized_quantity

        sum_quantities = sum(trans.get_spot_quantity() for trans in buy_transactions)
        sum_costs = sum(trans.get_current_cost() for trans in buy_transactions)

        coin_data.current_average_cost = sum_costs / sum_quantities
        coin_data._buy_transactions_data = buy_transactions
