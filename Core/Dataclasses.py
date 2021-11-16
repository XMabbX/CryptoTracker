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
    return time + '_' + str(operation_type) + '_' + coin_tick + '_' + str(transaction.quantity).replace('.',
                                                                                                        '_').replace(
        '-', '')


@dataclass
class Coin:
    coin_info: CoinInfo
    transactions: List[Transaction] = field(repr=False)


@dataclass(frozen=True)
class CoinData:
    coin: Coin
    spot_quantity: Decimal
    earn_quantity: Decimal
    current_value_per_unit: Decimal
    current_average_cost: Decimal
    total_costs: Decimal
    total_current_cost: Decimal
    total_unrealized_gains: Decimal
    current_total_value: Decimal
    total_realized_gains: Decimal
    total_realized_value: Decimal

    buy_transactions_data: List['BuyTransactionData']
    fees_data: 'FeeData'
    coin_earn: 'CoinEarn'

    def print_status(self):
        print(f"-------------------------------------------------------------------------------------------")
        print(f"Coin info: {self.coin}")
        if self.current_value_per_unit:
            print(f"Current quantity in stock: {self.spot_quantity}; "
                  f"Current value: {self.spot_quantity * self.current_value_per_unit}")
            print(f"Current quantity in earn: {self.earn_quantity}; "
                  f"Current value: {self.earn_quantity * self.current_value_per_unit}")
        else:
            print(f"Current quantity in stock: {self.spot_quantity}; Current value: Not available")
            print(f"Current quantity in earn: {self.earn_quantity}; Current value: Not available")

        print(f"Total quantity spent in fees: {self.fees_data.total_cost}€")
        print(f"Total value earn:")
        print(f"{self.coin_earn}")
        print(f"Current earn coins: {self.coin_earn.current_quantity}")
        print(f"Current earn value: {self.coin_earn.current_value}")
        print(f"Total realized gains from earn coins: {self.coin_earn.realized_gains}")
        print(f"Current average cost: {self.current_average_cost}")
        print(
            f"Change respect average cost: {((self.current_value_per_unit - self.current_average_cost) / self.current_average_cost):.2%}")

        print(f"Buy transactions cost and gains")
        for buy_trans in self.buy_transactions_data:
            print(buy_trans)

        print(f"Total costs: {self.total_costs}")
        print(f"Total current value: {self.current_total_value}")
        print(f"Total current costs: {self.total_current_cost}")
        print(f"Total unrealized gains: {self.total_unrealized_gains}")
        print(f"Total realized gains: {self.total_realized_gains}")
        print(f"Total realized value: {self.total_realized_value}")


@dataclass
class Amortization:
    quantity: Decimal
    total_value: Decimal


@dataclass
class FeeTransaction:
    transaction: Transaction = field(repr=False)
    cost_per_unit: Decimal
    cost: Decimal = field(init=False)

    def __post_init__(self):
        self.cost = self.transaction.quantity * self.cost_per_unit


@dataclass
class CoinEarn:
    coin: Coin
    total_earn_quantity: Decimal

    current_conversion_rate: Decimal = field(init=False)
    amortized_quantities: List[Amortization] = field(init=False)

    def __post_init__(self):
        self.amortized_quantities = []

    # TODO I have to remove this from the coin Earn and use a callback to the _CoinData object???
    def update_current_conversion_rate(self, conversion_rate):
        self.current_conversion_rate = conversion_rate

    def add_amortized(self, quantity: Decimal, total_value: Decimal):
        self.amortized_quantities.append(Amortization(quantity, total_value))

    @property
    def total_current_value(self):
        return self.current_conversion_rate * self.total_earn_quantity

    @property
    def current_quantity(self):
        return self.total_earn_quantity - sum(x.quantity for x in self.amortized_quantities)

    @property
    def current_value(self):
        return self.current_quantity * self.current_conversion_rate

    @property
    def realized_gains(self):
        return sum(x.total_value for x in self.amortized_quantities)


class BuyTransactionData:

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
    def unrealized_gains_change_percentage_string(self):
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
    def realized_gains_change_percentage_string(self):
        return "{0:.2%}".format(self.realized_gains_change_percentage)

    def add_amortized(self, quantity: Decimal, total_value: Decimal):
        self.amortized_quantities.append(Amortization(quantity, total_value))


class FeeData:

    def __init__(self):
        self.transactions_list: List[FeeTransaction] = []

    def create_fee_transaction(self, transaction, cost_per_unit):
        self.transactions_list.append(FeeTransaction(transaction, cost_per_unit))

    @property
    def total_quantity(self):
        return sum(x.transaction.quantity for x in self.transactions_list)

    @property
    def total_cost(self):
        return sum(x.cost for x in self.transactions_list)
