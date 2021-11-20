import pandas as pd
from typing import List
from pathlib import Path
from decimal import Decimal

from Core.Dataclasses import ProtoTransaction


class BinanceCSVReader:

    @classmethod
    def import_directory(cls, directory: [str, Path]):
        print("Starting importing directory")
        directory = cls._convert_to_path(directory)

        csv_files = directory.glob('*.csv')

        transactions_list = []
        for file in csv_files:
            transactions_list.extend(cls.import_file(file))

        print(f"Finished importing all files. Generated {len(transactions_list)} transactions")
        return transactions_list

    @classmethod
    def import_file(cls, file: [str, Path]) -> List[ProtoTransaction]:
        file = cls._convert_to_path(file)
        print(f"Importing file {file}")
        print(f"Reading file...")
        data = pd.read_csv(file, infer_datetime_format=True, parse_dates=['UTC_Time'], dtype={'Change': str})
        print(f"Finished read file")
        transactions_list = cls._parse_data(data)
        print(f"Finished parsing data")

        return transactions_list

    @staticmethod
    def _convert_to_path(string: [str, Path]) -> Path:
        if isinstance(string, str):
            return Path(string)
        return string

    @classmethod
    def _parse_data(cls, data: pd.DataFrame) -> List[ProtoTransaction]:
        print("Parsing data")
        total_rows = len(data)
        transactions_list = []
        for idx, row in data.iterrows():
            # print(f"Parsing transaction {idx} of {total_rows}")
            transactions_list.append(cls._parse_entry(row))
        return transactions_list

    @classmethod
    def _parse_entry(cls, row: pd.Series) -> ProtoTransaction:
        # ['User_ID', 'UTC_Time', 'Account', 'Operation', 'Coin', 'Change', 'Remark']
        utc_time = pd.to_datetime(row['UTC_Time'])
        value = Decimal(row['Change'])
        transaction_type = cls._get_transaction_type(row['Operation'], value)
        coin = row['Coin']
        # TODO add list of execptions
        if coin == 'LDFTM':
            coin = 'FTM'
        account = row['Account']

        return ProtoTransaction(value, coin, transaction_type, utc_time, account)

    @staticmethod
    def _get_transaction_type(transaction_name: str, value: float) -> ProtoTransaction.TransactionType:
        # ['Deposit' 'Fee' 'Transaction Related' 'Buy' 'POS savings purchase',
        # 'POS savings interest' 'Sell' 'POS savings redemption', 'Liquid Swap add/sell']

        if transaction_name in ("The Easiest Way to Trade", "Small assets exchange BNB", "Transaction Related",):
            transaction_name = "Sell" if value < 0 else "Buy"

        if transaction_name == 'Buy':
            return ProtoTransaction.TransactionType.BUY
        elif transaction_name == 'Sell':
            return ProtoTransaction.TransactionType.SELL
        elif transaction_name == 'Deposit':
            return ProtoTransaction.TransactionType.DEPOSIT
        elif transaction_name == 'Fee':
            return ProtoTransaction.TransactionType.FEE
        elif transaction_name == 'Savings purchase':
            return ProtoTransaction.TransactionType.SAVING_PURCHASE
        elif transaction_name == 'Savings Interest':
            return ProtoTransaction.TransactionType.SAVING_INTEREST
        elif transaction_name == 'Savings Principal redemption':
            return ProtoTransaction.TransactionType.SAVING_REDEMPTION
        elif transaction_name == 'POS savings purchase':
            return ProtoTransaction.TransactionType.POS_PURCHASE
        elif transaction_name == 'POS savings interest':
            return ProtoTransaction.TransactionType.POS_INTEREST
        elif transaction_name == 'POS savings redemption':
            return ProtoTransaction.TransactionType.POS_REDEMPTION
        elif transaction_name == 'Liquid Swap add/sell':
            return ProtoTransaction.TransactionType.LIQUID_SWAP_ADD
        elif transaction_name == 'Liquid Swap rewards':
            return ProtoTransaction.TransactionType.LIQUID_SWAP_REDEMPTION
        else:
            raise ValueError(f"Operation name not found: {transaction_name}")
