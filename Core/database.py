from typing import Dict, List

from .Dataclasses import Coin, Transaction
from .CoinAPIExternal import CoinAPI
from .Dataclasses import ProtoTransaction


class CoinData:

    def __init__(self, coin):
        self._coin = coin


class DataBase:
    name: str
    holding: Dict[str, Coin]
    transactions: Dict[str, Transaction]


class DataBaseAPI:

    def __init__(self, database=None):
        if database is None or not isinstance(database, DataBase):
            raise ValueError("A database must exist")
        self._database = database
        self._duplicate_ids = set()

    @staticmethod
    def create_new_database(name='default'):
        db = DataBase()
        db.name = name
        db.holding = {}
        db.transactions = {}
        return db

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
