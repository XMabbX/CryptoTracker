from typing import Tuple

from PyQt5 import QtWidgets
from PyQt5.QtCore import QSize

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
                self._format_decimal(coin_data.spot_quantity + coin_data.earn_quantity),
                self._format_decimal(coin_data.current_total_value, fiat_name))

    def _get_coin_name(self, coin_data):
        coin_info = coin_data.coin.coin_info
        return f"{coin_info.tick}({coin_info.name})"


class OverviewContext:

    def __init__(self, layout):
        self._layout = layout
        self._widget_list = []

    def create_contents(self):
        _header = QtWidgets.QLabel('Overview')
        _header.setFont(TextProperties.title_font())
        self._widget_list.append(_header)
        self._widget_list.append(self._create_coin_info_rows())

    def _create_coin_info_rows(self):
        app = cryptoApp.get_instance()
        return Table([CoinDataItem(app.get_coin_data(coin)) for coin in app.get_list_all_coins()])

    def show(self):
        for widget in self._widget_list:
            self._layout.addWidget(widget)
        self._layout.addStretch()

    def hide(self):
        for widget in self._widget_list:
            widget.hide()


class LoadingContext:

    def __init__(self, layout):
        self._layout = layout
        self._widget_list = []

    def create_contents(self):
        header = QtWidgets.QLabel('Overview')
        header.setFont(TextProperties.title_font())
        self._widget_list.append(header)
        self._widget_list.append(QtWidgets.QLabel("Loading data ..."))
        app = cryptoApp.get_instance()
        app.process_all_coin_data()

    def show(self):
        for widget in self._widget_list:
            self._layout.addWidget(widget)
        self._layout.addStretch()

    def hide(self):
        for widget in self._widget_list:
            widget.hide()
