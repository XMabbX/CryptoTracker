from typing import List, Iterable

from PyQt5 import QtWidgets
from PyQt5.QtCore import QSize, Qt


class RowItem(list):

    def __init__(self, dataclass_type, *args, **kwargs):
        super(RowItem, self).__init__(*args, **kwargs)
        self._dataclass_type = dataclass_type

    def _format_decimal(self, value, fiat):
        return f"{value:.12g} {fiat}"

    def get_columns_names(self):
        raise NotImplementedError

    def get_columns_num(self):
        return len(self)


class Table(QtWidgets.QTableWidget):

    def __init__(self, children: List[RowItem], fixed_type=None):
        super().__init__()
        self.verticalHeader().setVisible(False)
        self.current_row_count = 0
        self.current_columns_count = 0
        self.last_row = 0
        header = self.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self._fixed_type = fixed_type
        self._assert_row_item_type(children)
        self._children = children
        self._update_table_size()

        self.setHorizontalHeaderLabels(self._children[0].get_columns_names())
        for current_child in children:
            self._add_row(current_child)

    def sizeHint(self):
        horizontal = self.horizontalHeader()
        vertical = self.verticalHeader()
        frame = self.frameWidth() * 2
        return QSize(horizontal.length() + vertical.width() + frame,
                     vertical.length() + horizontal.height() + frame)

    def _assert_row_item_type(self, children):
        if self._fixed_type is None:
            self._fixed_type = type(children)

        assert all(isinstance(current_child, self._fixed_type) for current_child in children)

    def _update_table_size(self):
        self.current_row_count = len(self._children)
        self.current_columns_count = max(child.get_columns_num() for child in self._children)
        self.setRowCount(self.current_row_count)
        self.setColumnCount(self.current_columns_count)

    def _add_row(self, row_item: RowItem):
        for idx, item in enumerate(row_item):
            item = QtWidgets.QTableWidgetItem(item)
            item.setFlags(Qt.ItemIsEditable)
            self.setItem(self.last_row, idx, item)
        self.last_row += 1

    def add_rows(self, rows: Iterable[RowItem]):
        self._assert_row_item_type(rows)
        self._children.extend(rows)
        self._update_table_size()
        for row_item in rows:
            self._add_row(row_item)
