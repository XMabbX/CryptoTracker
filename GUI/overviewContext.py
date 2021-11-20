from typing import Tuple

from PyQt5 import QtWidgets
from . import cryptoApp
from .textProperties import TextProperties
from .table import Table, RowItem
from Core.Dataclasses import CoinData


class CoinDataItem(RowItem):

    def __init__(self, coin_data: CoinData):
        self._coin_data = coin_data
        self._collected_data = self._collect_data(coin_data)
        super(CoinDataItem, self).__init__(CoinData, self._collected_data)

    def get_columns_names(self) -> Tuple:
        return 'Coin', 'Total Quantity', 'Total value'

    def _collect_data(self, coin_data: CoinData) -> Tuple:
        fiat_name = cryptoApp.get_instance().get_base_fiat()
        return (self._get_coin_name(coin_data),
                self._format_decimal(coin_data.spot_quantity + coin_data.earn_quantity, fiat_name),
                self._format_decimal(coin_data.current_total_value, fiat_name))

    def _get_coin_name(self, coin_data):
        coin_info = coin_data.coin.coin_info
        return f"{coin_info.tick}({coin_info.name})"


class OverviewContext:

    layout = None

    def create_contents(self):
        if self.layout:
            return self.layout
        _header = QtWidgets.QLabel('Overview')
        _header.setFont(TextProperties.title_font())
        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(_header)
        self.layout.addWidget(self._create_coin_info_rows())
        self.layout.addStretch()
        return self.layout

    def _create_coin_info_rows(self):
        app = cryptoApp.get_instance()
        app.process_coin_data('BTC')
        coin_data = app.get_coin_data('BTC')
        coins_list = [CoinDataItem(coin_data)]
        self.table = Table(coins_list)
        return self.table

    def hide(self):
        if self.layout:
            self.layout.hide()
