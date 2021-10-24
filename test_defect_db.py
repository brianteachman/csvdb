#!/usr/bin/env python3
# =============================================================================
# Author:  Brian Teachman
# Date:    10/22/2021
# =============================================================================
"""Test cases for the DefectDB module

    Run this test using this command:

        python -m unittest test_defect_db.py

    Read more here: https://docs.python.org/3/library/unittest.html
"""
import unittest
import pandas as pd
from csvdb import DataManager


class TestDefectDb(unittest.TestCase):

    # Example csv entry: 999999999999,A1,CC,Operator,Station 1,9999
    test_defect = {
        'panel_id': '99999999999x',
        'location': 'A1',
        'defect_type': 'CC',
        'cause': 'Operator',
        'origin': 'Bussing Station',
        'uid': 9999
    }

    def setUp(self):
        # Did you create 'data/datatable.csv' ?
        self.baseline = pd.read_csv('data/datatable.csv')
        self.db = DataManager('datatable.csv', 'data')
        self.data_size = len(self.baseline)

    def test_row_count(self):
        self.assertEqual(self.db.row_count(), self.data_size, 'incorrect number of rows')

    def test_contains(self):
        self.assertTrue(self.db.contains(5), 'UID #5 definitely exists')
        self.assertTrue(self.db.contains(15), 'UID #15 definitely exists')
        self.assertFalse(self.db.contains(20), 'Our dataset has 17 entries, not 20')
        self.assertTrue(self.db.contains('CC', 'defect_type'), 'Cells cracks are pretty common')
        self.assertFalse(self.db.contains('Nuclear Meltdown', 'defect_type'), 'No nukes here')
        self.assertFalse(self.db.contains('Monkey', 'cause'), 'A monkey?')
        self.assertTrue(self.db.contains('Machine', 'cause'), 'A monkey?')

    def test_count(self):
        self.assertEqual(self.db.count('MS', 'defect_type'), 6, 'There are actually 6 missed solders in the dataset')
        self.assertEqual(self.db.count('Nuclear Meltdown', 'defect_type'), 0, '0 nukes here')

    def test_insert_row(self):
        self.db.insert_row(self.test_defect)
        self.assertEqual(self.db.row_count(), self.data_size + 1, 'Does not have added row')
        self.assertTrue(self.db.contains(self.test_defect['uid']), 'No rows contain added UID')

    def test_delete_row(self):
        self.db.delete_row(5)
        self.assertEqual(self.db.row_count(), self.data_size - 1, 'Incorrect number of rows')
        self.assertFalse(self.db.contains(5), 'UID #5 should not exist here since we deleted it')
        # self.assertFalse(5 in self.db._data.uid)

    def test_delete_rows_where(self):
        self.db.delete_rows_where('MS', 'defect_type')  # There are 6 of these in our dataset
        self.assertEqual(self.db.row_count(), self.data_size - 6, 'Incorrect number of rows')

    def test_delete_row_where_uid_doesnt_exist(self):
        self.db.delete_row(self.data_size + 1)  # Add one more than exist in the set
        self.assertEqual(self.db.row_count(), self.data_size, 'Nothing deleted, should have same number of rows')

    @unittest.skip
    def test_save_db(self):
        self.fail()


if __name__ == '__main__':
    unittest.main()
