from dataclasses import dataclass, field
from typing import List
from enum import Enum
from decimal import Decimal
from datetime import datetime


@dataclass(frozen=True)
class CoinInfo:
    tick: str
    name: str


class TransactionType(Enum):
    DEPOSIT = 0
    WITHDRAWAL = 1
    BUY = 2
    SELL = 3
    FEE = 4
    POS_PURCHASE = 5
    POS_INTEREST = 6
    POS_REDEMPTION = 7
    SAVING_PURCHASE = 8
    SAVING_INTEREST = 9
    SAVING_REDEMPTION = 10
    LIQUID_SWAP_ADD = 11


@dataclass(frozen=True)
class ProtoTransaction:
    TransactionType = TransactionType

    value: Decimal
    coin_name: str
    operation_type: TransactionType
    UTC_Time: datetime
    account: str


@dataclass(frozen=True)
class Transaction:
    TransactionType = TransactionType

    quantity: Decimal
    coin: 'Coin'
    operation_type: TransactionType
    UTC_Time: datetime
    account: str

    id: str = field(init=False)

    def __post_init__(self):
        super().__setattr__('id', _generateId(self))

    def __eq__(self, other):
        return other.id == self.id

    def __hash__(self):
        return self.id


def _generateId(transaction: Transaction):
    time = str(hex(int(transaction.UTC_Time.strftime('%Y%m%d%H%M%S%f'))))
    operation_type = hex(transaction.operation_type.value)
    coin_tick = transaction.coin.coin_info.tick
    return time + '_' + str(operation_type) + '_' + coin_tick + '_' + str(transaction.quantity).replace('.', '_').replace('-', '')


@dataclass
class Coin:
    coin_info: CoinInfo
    transactions: List[Transaction] = field(repr=False)
