#!/usr/bin/env python3
# =============================================================================
# Author:  Brian Teachman
# Date:    10/21/2021
# =============================================================================
"""Test cases for the DefectDB module

"""
# from unittest import TestCase
import unittest
import pandas as pd
from csvdb import DefectDb

# https://docs.python.org/3/library/unittest.html


class TestDefectDb(unittest.TestCase):

    def setUp(self):
        self.baseline = pd.read_csv('data/datatable.csv')
        self.db = DefectDb('datatable.csv', 'data')
        self.data_size = len(self.baseline)

    def test_row_count(self):
        self.assertEqual(self.db.row_count(), self.data_size, 'incorrect number of rows')
        self.assertEqual(self.db.row_count(), 13, 'incorrect number of rows')

    def test_delete_row(self):
        self.db.delete_row(5)
        self.assertEqual(self.db.row_count(), self.data_size - 1, 'incorrect number of rows')
        self.assertFalse(5 in self.db._data.uid)

    def test_delete_row_where_uid_doesnt_exist(self):
        self.db.delete_row(15)
        self.assertEqual(self.db.row_count(), 13, 'incorrect number of rows')

    def test_delete_rows_where(self):
        self.fail()

    def test_insert_row(self):
        self.fail()

    def test_save_db(self):
        self.fail()


if __name__ == '__main__':
    unittest.main()
