from datetime import datetime
from unittest import TestCase
from unittest import mock
from decimal import Decimal

import pandas as pd
from freezegun import freeze_time

from ..CoinAPIExternal import BinanceAPI


class MockClient:

    def __init__(self):
        pass


def _create_mock_client(self, keys):
    return MockClient()


def _create_fake_pairs(self, path):
    return pd.DataFrame([['USDTEUR', 'USDT', 'EUR']],
                        columns=['Symbol', 'First', 'Second'],
                        )


def _mock_get_conversion(self, symbol, date):
    return Decimal(10)


@freeze_time("2021-01-01 00:00:00")
@mock.patch.object(BinanceAPI, "_readKeys", return_value={"": ""})
@mock.patch.object(BinanceAPI, "_create_client", new=_create_mock_client)
@mock.patch.object(BinanceAPI, "_check_pairs_cache", new=_create_fake_pairs)
@mock.patch.object(BinanceAPI, "_get_conversion", new=_mock_get_conversion)
class TestBinanceAPI(TestCase):

    def _create_api(self):
        return BinanceAPI("", "")

    def test_createBinanceAPI(self, *mocked_methods):
        self._create_api()

    def test_get_conversion(self, *mocked_methods):
        api = self._create_api()
        assert api.get_conversion_rate('USDT', 'EUR', datetime.now()) == Decimal(10)

    def test_get_inverse_conversion(self, *mocked_methods):
        api = self._create_api()
        rate = api.get_conversion_rate('EUR', 'USDT', datetime.now())
        self.assertAlmostEqual(float(rate), 0.1)
