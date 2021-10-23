from typing import Dict, List
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
        self.stock = Decimal(0)
        self.earn = Decimal(0)
        self._average_cost = None
        self._transactions = None

        self._collect_stock_value()
        self._collect_earn_value()

    def _collect_stock_value(self):
        for trans in self.coin.transactions:
            if trans.operation_type in IN_STOCK_OPERATIONS:
                self.stock += trans.value
            else:
                raise ValueError(f"Transaction operation {trans.operation_type} not listed")

    def _collect_earn_value(self):
        for trans in self.coin.transactions:
            if trans.operation_type in IN_EARN_OPERATIONS:
                self.earn -= trans.value


@dataclass
class TransactionData:
    # TODO save cost per unit
    # TODO save current value per unit
    transaction: Transaction
    cost: Decimal
    current_value: Decimal
    change: float = field(init=False)
    change_value: Decimal = field(init=False)
    change_percentage: str = field(init=False)

    def __post_init__(self):
        self.change_value = self.current_value - self.cost
        self.change = float(self.change_value / self.cost)
        self.change_percentage = "{0:.2%}".format(self.change)


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
    holding: Dict[str, Coin]
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

    @staticmethod
    def create_new_database(name='default'):
        db = DataBase()
        db.name = name
        db.holding = {}
        db.transactions = {}
        return db

    def printStatus(self):
        for coin_tick, coin in self._database.holding.items():
            coin_data = CoinData(coin)
            print(f"Coin: {coin.coin_info.tick}. Current stock: {coin_data.stock}. Current earn: {coin_data.earn}")

    def acknowledge_duplicates(self, path):
        with open(path, 'r') as f:
            for line in f.readlines():
                self._duplicate_ids.add(line.strip())

    def add_coin(self, coin_name: str) -> Coin:
        if coin_name in self._database.holding:
            raise KeyError(f"The coin {coin_name} already exist in the database")

        try:
            coin_info = CoinAPI.get_coin_info(coin_name)
        except KeyError:
            raise KeyError(f"The coin {coin_name} doesn't exist")

        new_coin = Coin(coin_info, [])

        self._database.holding[coin_name] = new_coin
        return new_coin

    def remove_coin(self, coin_name: str, force: bool = False):
        try:
            coin = self._database.holding[coin_name]
        except KeyError:
            raise KeyError(f"The coin {coin_name} doesn't exist in the database")

        if force is False and len(coin.transactions) > 1:
            raise ValueError(f"The coin {coin_name} has transactions is not safe to delete it")

        if len(coin.transactions) > 1:
            for transaction in coin.transactions:
                del self._database.transactions[transaction.id]

        del self._database.holding[coin_name]

    def get_coin(self, coin_name: str) -> Coin:
        return self._database.holding[coin_name]

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

        # if len(list_transactions) == len(set(x.id for x in list_transactions)):
        #     pass

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

        return Transaction(proto.value, coin, proto.operation_type, proto.UTC_Time, proto.account)

    def _validate_transaction(self, transaction: Transaction) -> bool:
        if transaction.id in self._database.transactions:
            return False
        return True

    def add_transaction(self, transaction: [Transaction, List[Transaction]]):
        if isinstance(transaction, list):
            for element in transaction:
                self.add_transaction(element)

            return None

        if transaction.coin.coin_info.tick not in self._database.holding:
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

    def process_transactions_data(self, transactions: List[Transaction]):
        transaction_data = []
        for trans in transactions:
            cost = trans.value * self._external_api.get_conversion_rate(trans.coin.coin_info.tick, self._return_fiat,
                                                                        trans.UTC_Time)
            current_value = trans.value * self._external_api.get_conversion_rate(trans.coin.coin_info.tick,
                                                                                 self._return_fiat,
                                                                                 self._get_now_time())
            new_data = TransactionData(trans, cost, current_value)
            transaction_data.append(new_data)
        return transaction_data

    def collect_all_earn_earnings(self, transactions: List[Transaction]) -> Dict[str, CoinEarn]:
        coin_dict = {}
        for trans in transactions:
            if trans.operation_type in (trans.TransactionType.POS_INTEREST, trans.TransactionType.SAVING_INTEREST):
                coin_tick = trans.coin.coin_info.tick
                if coin_tick in coin_dict:
                    coin_dict[coin_tick].quantity += trans.value
                else:
                    coin_dict[coin_tick] = CoinEarn(coin=trans.coin, quantity=trans.value)

        for coin_tick, coin in coin_dict.items():
            conversion_rate = self._external_api.get_conversion_rate(coin_tick, self._return_fiat, self._get_now_time())
            coin.compute_current_value(conversion_rate)

        return coin_dict
