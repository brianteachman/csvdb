#!/usr/bin/env python3
# =============================================================================
# License: WTFPL
# =============================================================================
"""A CSV database abstraction layer (DAL) loosely based on the Data Mapper pattern."""

import pandas as pd
import csv
import os
from stat import S_IREAD, S_IWUSR, S_IRGRP, S_IROTH
from datetime import date


def _lock_file(filename, unlock=False) -> bool:
    """
    This method locks the currently set CSV datafile.

    :param unlock: bool   If is_writable it True, this function locks the file.
    :return:       bool   Whether or not the file is locked.
    """
    is_locked = True
    if os.path.isfile(filename):
        if not unlock:
            # Change permission of database to Read Only
            os.chmod(filename, S_IREAD | S_IRGRP | S_IROTH)
        else:
            # If exist, change permissions of database to Read & Write for User
            os.chmod(filename, S_IWUSR | S_IREAD)
            is_locked = False
    return is_locked


class DataManager:
    """Data manager is a DAL class based on pandas.DataFrame which persist data in a CSV file.

    1. First column is the panels ID.
    2. Panel ID can exist in more than one entry
    """

    def __init__(self, file_name: str, file_dir: str, uid: str):
        self._data_file = file_name
        self._data_dir = file_dir
        self.default_id_col = uid
        self._data = self._get_from_csv()

    def _get_uri(self, filename=None, path=None, date_stamp=False, as_backup=False):
        """
        Prepare a string filename of CSV file to load or save. Defaults to file set on init.

        :param filename:   str   Name of CSV file to save to.
        :param path:       str   Path of directory to save CSV file in.
        :param date_stamp: bool  Add Date to saved file name?
        :param as_backup:  bool  Prepend '.bak' to file name?
        :return:           str   The full path to a file on disk
        """
        file_dir = path if path else self._data_dir
        file_name = filename if filename else self._data_file
        if date_stamp:
            save_file = f'{file_dir}/{date.today()}-{file_name}'
        else:
            save_file = f'{file_dir}/{file_name}'
        if as_backup:
            save_file = f'{save_file}.bak'
            # TODO: Compress backup file
        return save_file

    def _get_from_csv(self, filename=None, path=None) -> pd.DataFrame:
        """
        Reads a CSV file into memory.

        Note: this method is destructive as it overwrites self.data

        :param filename: str   Name of CSV file to load.
        :param path:     str   Path of directory the CSV file is located.
        :return: pd.DataFrame
        """
        # Do not automatically not prepended a date stamp to the master data set.
        return pd.read_csv(self._get_uri(filename, path))

    def _save_to_csv(self, filename) -> None:
        """
        Wrapper method for pandas.DataFrame.to_csv().

        :param filename: str   Name of CSV file to save to.
        :return:         None
        """
        _lock_file(filename, unlock=True)
        self._data.to_csv(filename, index=False)
        _lock_file(filename)

    def save(self, filename=None, path=None, date_stamp=True, as_backup=False) -> None:
        """
        Prepares filename and calls method to save entire DB to CSV file.

        :param filename:   str   Name of CSV file to save to.
        :param path:       str   Path of directory to save CSV file in.
        :param date_stamp: bool  Add Date to saved file name?
        :param as_backup:  bool  Prepend '.bak' to file name?
        :return:           None
        """
        save_file = self._get_uri(filename, path, date_stamp, as_backup)
        self._save_to_csv(save_file)
        # TODO: Setup logging

    def insert(self, entries) -> int:
        """
        Insert row into in-memory and to disk database.

        Example csv/json entry: 99999999999x,A1,CC,Operator,Bussing Station,9999

        :param entries: [
            [99999999999x,A1,CC,Operator,Bussing Station,9998],
            [99999999999x,A2,CC,Operator,Bussing Station,9999]
        ]
        :return: int    The UID (int) of the newly created entry, or 0 if failed
        """
        uid = 0
        _lock_file(self._get_uri(date_stamp=True), unlock=True)
        with open(self._get_uri(date_stamp=True), 'a', newline='', encoding='utf-8') as csv_file:
            csvwriter = csv.writer(csv_file)
            for row in entries:

                # Add UID to entry
                uid = self.get_next_uid()
                row.append(uid)

                # Append the data to the in-memory data structure
                self._data.loc[len(self._data.index)] = row

                # Append the data to the file on disk
                csvwriter.writerow(row)
        _lock_file(self._get_uri(date_stamp=True))
        return uid

    def get_last_n_rows(self, n) -> pd.DataFrame:
        return self._data.tail(n)

    def edit(self, dataset):
        """

        """
        return None

    def delete_where(self, value, col_name) -> bool:
        """
        Inplace: drop row(s) where col_name=value

        :param col_name:
        :param value:
        :return: bool    Whether or not entry was removed
        """
        has_value = self.contains(value, col_name)
        if has_value:
            self._data.drop(self._data[self._data[col_name] == value].index, inplace=True)
            self.save()
        return has_value

    def delete(self, uid: int) -> bool:
        """
        Inplace: drop row where default UID column is equal to uid

        :param uid: int   The Unique row ID
        :return:    bool  Whether or not entry was removed
        """
        return self.delete_where(uid, self.default_id_col)

    def info(self) -> None:
        """ Wrapper method for pandas.DataFrame.info()."""
        self._data.info()

    def dump(self) -> None:
        """ Print pandas.DataFrame to the stream in readable format."""
        print(self._data)

    def to_json(self):
        return self._data.to_json(orient="values")

    def contains(self, value, col_name=None) -> bool:
        """Efficiently check for a value. 'uid' is the default column.

        :param value:
        :param col_name:
        :return: bool
        """
        if not col_name:
            col_name = self.default_id_col
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

    def get_next_uid(self) -> int:
        return int(self._data.iloc[-1][self.default_id_col]) + 1
