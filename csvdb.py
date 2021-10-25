#!/usr/bin/env python3
# =============================================================================
# Author:  Brian Teachman
# Date:    10/22/2021
# =============================================================================
"""Database abstraction layer (DAL) based on the Data Mapper pattern.

"""

import pandas as pd
import os
from stat import S_IREAD, S_IWUSR, S_IRGRP, S_IROTH
from datetime import date


class DataManager:
    """Data manager is a DAL class based on pandas.DataFrame which persist data in a CSV file.

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

        :param filename: str   Name of CSV file to load.
        :param path:     str   Path of directory the CSV file is located.
        :return: pd.DataFrame
        """
        file_name = f"{self._dataDir}/{self._dataFile}"
        return pd.read_csv(file_name)

    def _save_to_csv(self, filename):
        """
        Wrapper method for pandas.DataFrame.to_csv().

        :param filename: str   Name of CSV file to save to.
        :return:         None
        """
        # If exist, change permissions of database to Read & Write for User
        if os.path.isfile(filename):
            os.chmod(filename, S_IWUSR|S_IREAD)

        self._data.to_csv(filename, index=False)

        # Change permission of database to Read Only
        os.chmod(filename, S_IREAD|S_IRGRP|S_IROTH)

    def save(self, filename=None, path=None, date_stamp=True, as_backup=False) -> None:
        """
        Prepares filename and calls method to save DB to CSV file.

        :param filename:   str   Name of CSV file to save to.
        :param path:       str   Path of directory to save CSV file in.
        :param date_stamp: bool  Add Date to saved file name?
        :param as_backup:  bool  Prepend '.bak' to file name?
        :return:           None
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
        # print(f'LOG: You just wrote the backup file: {save_file}')

    def insert(self, defect_data):
        """
        Insert row into in-memory database.

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

    def edit(self, dataset):
        """

        """
        return None

    def delete_where(self, value, col_name) -> bool:
        """
        Inplace: drop row(s) where col_name=value

        :param col_name:
        :param value:
        :return: None
        """
        hasValue = self.contains(value, col_name)
        if hasValue:
            self._data.drop(self._data[self._data[col_name] == value].index, inplace=True)
            self.save()
        return hasValue

    def delete(self, uid: int) -> bool:
        """
        Inplace: drop row where UID column is equal to uid

        :param uid:
        :return:
        """
        hasUID = self.contains(uid)
        if hasUID:
            self.delete_where(uid, 'uid')
        return hasUID

    def info(self) -> None:
        """ Wrapper method for pandas.DataFrame.info()."""
        self._data.info()

    def dump(self) -> None:
        """ Print pandas.DataFrame to the stream in readable format."""
        print(self._data)

    def contains(self, value, col_name='uid') -> bool:
        """Efficiently check for a value. 'uid' is the default column.

        :param value:
        :param col_name:
        :return: bool
        """
        return self._data[col_name].isin([value]).any()

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
