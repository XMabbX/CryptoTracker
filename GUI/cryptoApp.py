import os
import json
from typing import Optional
from PyQt5 import QtWidgets, QtCore

from API.CSVReader import BinanceCSVReader
from Core.CoinAPIExternal import BinanceAPI
from Core.database import DataBaseAPI, TransactionValidator
from GUI.overviewContext import OverviewContext

class MyQThread(QtCore.QThread):

    on_update = QtCore.pyqtSignal(str)
    on_complete = QtCore.pyqtSignal(int)

    def __init__(self, target, parent=None):
        super(MyQThread, self).__init__(parent)
        # You can change variables defined here after initialization - but before calling start()
        self.completionMessage = "done."
        self._target = target
        self._update_method = lambda x: self.on_update.emit(x)

    def run(self):
        self._target(self._update_method)
        self.on_complete.emit(0)


class CryptoTrackerApp(QtWidgets.QApplication):

    def __init__(self, *args, **kwargs):
        super(CryptoTrackerApp, self).__init__(*args, **kwargs)
        self._config = self._read_config()
        self._externalAPI = BinanceAPI(self._config.get_config_value('keys_path'),
                                       self._config.get_config_value('cache_folder'))
        self._db = DataBaseAPI.create_new_database(self._config.get_config_value('database_name'))
        self._base_fiat = 'EUR'
        self._db_api = DataBaseAPI(self._db, self._externalAPI, self._base_fiat, now_precision=DataBaseAPI.Precision.H1)
        self._validator = TransactionValidator(self._db_api, self._config.get_config_value('duplicate_whitelist'))

    def create_contents(self):
        self.main_window = Window()
        self.main_window.show()

    def activate_context(self, context_cls):
        self.main_window.activate_context(context_cls)

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

    def process_all_coin_data(self):
        process_coins_thread = MyQThread(self._db_api.process_all_coins_data)
        process_coins_thread.start()
        process_coins_thread.on_update.connect(self.main_window.update_status_bar)
        process_coins_thread.on_complete.connect(self._done_process)

    def _done_process(self, return_code):
        if return_code == 0:
            self.main_window.update_status_bar("Done.")
            self._show_all_data()
        else:
            self.main_window.update_status_bar("Error.")

    def _show_all_data(self):
        print("Showing all data")
        self.activate_context(OverviewContext)

    def get_list_all_coins(self):
        return self._db_api.get_coin_list()


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
        self.setSizePolicy(QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.MinimumExpanding,
                                                 QtWidgets.QSizePolicy.MinimumExpanding))
        self.currentContext = None
        self._centralWidget = QtWidgets.QWidget(self)
        self._create_status_bar()
        self.setCentralWidget(self._centralWidget)
        self._layout = QtWidgets.QVBoxLayout(self._centralWidget)

    def activate_context(self, context_cls, *args, **kwargs):
        if self.currentContext:
            self.currentContext.hide()
        self.currentContext = context_cls(self._layout, *args, **kwargs)
        self.currentContext.create_contents()
        self.currentContext.show()
        self.update_status_bar("Activated")

    def _create_status_bar(self):
        status = QtWidgets.QStatusBar()
        self.setStatusBar(status)
        self.update_status_bar("Done.")

    def update_status_bar(self, message):
        self.statusBar().showMessage(f"{type(self.currentContext)} - {message}")


def get_instance() -> Optional[CryptoTrackerApp]:
    return CryptoTrackerApp.instance()
