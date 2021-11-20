import os
import json
from typing import Optional
from PyQt5 import QtWidgets

from API.CSVReader import BinanceCSVReader
from Core.CoinAPIExternal import BinanceAPI
from Core.database import DataBaseAPI, TransactionValidator


class CryptoTrackerApp(QtWidgets.QApplication):

    def __init__(self, *args, **kwargs):
        super(CryptoTrackerApp, self).__init__(*args, **kwargs)
        self._config = self._read_config()
        self._externalAPI = BinanceAPI(self._config.get_config_value('keys_path'),
                                       self._config.get_config_value('cache_folder'))
        self._db = DataBaseAPI.create_new_database(self._config.get_config_value('database_name'))
        self._base_fiat = 'EUR'
        self._db_api = DataBaseAPI(self._db, self._externalAPI, self._base_fiat)
        self._validator = TransactionValidator(self._db_api, self._config.get_config_value('duplicate_whitelist'))

    def create_contents(self):
        self.main_window = Window()

    def activate_context(self, context_cls):
        self.main_window.activate_context(context_cls)
        self.main_window.show()

    def show(self):
        self.main_window.show()

    def _read_config(self):
        config_path = os.path.join(os.getcwd(), 'config.json')
        return Config(config_path)

    def load_csv_data(self):
        data_path = self._config.get_config_value('csv_folder')
        transaction_list = BinanceCSVReader.import_directory(data_path)
        new_transactions = self._validator.validate_and_parse_transactions(transaction_list)
        self._db_api.add_transaction(new_transactions)

    def get_base_fiat(self):
        return self._base_fiat

    def get_coin_data(self, coin_symbol):
        return self._db_api.get_coin_data(coin_symbol)

    def process_coin_data(self, coin_symbol):
        self._db_api.process_coin_data(coin_symbol)


class Config:

    def __init__(self, config_path):
        self._config_path = config_path
        with open(config_path) as f:
            self._config_data = json.load(f)

    def get_config_value(self, name):
        return self._config_data[name]

    def update_config_value(self, name, value):
        self._config_data[name] = value
        with open(self._config_path, 'w') as f:
            json.dump(self._config_data, self._config_path)


class Window(QtWidgets.QMainWindow):
    """Main Window."""

    def __init__(self, parent=None):
        """Initializer."""
        super().__init__(parent)
        self.setWindowTitle('CryptoTracker')
        self.setMaximumSize(1024, 512)
        self.currentContext = None
        self._centralWidget = QtWidgets.QWidget(self)
        self.setCentralWidget(self._centralWidget)

    def activate_context(self, context_cls, *args, **kwargs):
        if self.currentContext:
            self.currentContext.hide()
        self.currentContext = context_cls(*args, **kwargs)
        self._centralWidget.setLayout(self.currentContext.create_contents())


def get_instance() -> Optional[CryptoTrackerApp]:
    return CryptoTrackerApp.instance()
