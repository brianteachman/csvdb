#!/usr/bin/env python3
# =============================================================================
# Author:  Brian Teachman
# Date:    10/22/2021
# =============================================================================
"""Database abstraction layer (DAL) for a manufacturing production defects database.

"""

import pandas as pd
from datetime import date


class DataManager:
    """The Defects database is a DAL class based on pandas.DataFrame which persist data in a CSV file.

    1. First column is the panels ID.
    2. Panel ID can exist in more than one entry
    """

    def __init__(self, file_name: str, file_dir: str):
        self._dataFile = file_name
        self._dataDir = file_dir
        self._data = self._get_from_csv()

    def _get_from_csv(self, filename=None, path=None) -> pd.DataFrame:
        """
        Reads a CSV file into memory.

        Note: this method is destructive as it overwrites self.data

        :param filename:
        :param path:
        :return:
        """
        file_name = f"{self._dataDir}/{self._dataFile}"
        return pd.read_csv(file_name)

    def _save_to_csv(self, filename):
        """
        Wrapper method for pandas.DataFrame.to_csv().

        :param filename:
        :return:
        """
        self._data.to_csv(filename, index=False)

    def save_db(self, filename=None, path=None, date_stamp=True, as_backup=False) -> None:
        """

        :param filename:
        :param path:
        :param date_stamp:
        :param as_backup:
        :return:
        """
        file_dir = path if path else self._dataDir
        file_name = filename if filename else self._dataFile
        if date_stamp:
            save_file = f'{file_dir}/{date.today()}-{file_name}'
        else:
            save_file = f'{file_dir}/{file_name}'
        if as_backup:
            save_file = f'{save_file}.bak'
        self._save_to_csv(save_file)
        print(f'LOG: You just wrote the backup file: {save_file}')

    def delete_row(self, uid: int):
        """
        Inplace: drop row where UID column is equal to uid

        :param uid:
        :return:
        """
        self._data.drop(self._data[self._data.uid == uid].index, inplace=True)
        return False

    def delete_rows_where(self, value, col_name):
        """
        Inplace: drop row(s) where col_name=value

        :param col_name:
        :param value:
        :return: None
        """
        self._data.drop(self._data[self._data[col_name] == value].index, inplace=True)
        return False

    def insert_row(self, defect_data):
        """Insert row into in-memory database.

        Example csv entry: 99999999999x,A1,CC,Operator,Bussing Station,9999

        :param defect_data: {
            'panel_id': '99999999999x',
            'location': 'A1',
            'defect_type': 'CC',
            'cause': 'Operator',
            'origin': 'Bussing Station',
            'uid': 9999
        }
        :return: None
        """
        # self._data.append(pd.DataFrame(defect_data), ignore_index=True)
        self._data.loc[len(self._data.index)] = defect_data

    def contains(self, value, col_name='uid') -> bool:
        """Efficiently check for a value. 'uid' is the default column.

        :param value:
        :param col_name:
        :return: bool
        """
        return self._data[col_name].isin([value]).any()

    def info(self):
        """ Wrapper method for pandas.DataFrame.info()."""
        self._data.info()

    def dump(self):
        """ Print pandas.DataFrame to the stream in readable format."""
        print(self._data)

    def row_count(self) -> int:
        """ Return number of rows in the dataset (DataFrame)."""
        return self._data.shape[0]

    def count(self, value, col_name) -> int:
        """ Count the number of row entries containing value in a specific column.

        :param value:
        :param col_name:
        :return: int
        """
        target = self._data[self._data[col_name] == value]
        return target[col_name].count()
