#!/usr/bin/env python3
# =============================================================================
# License: WTFPL
# =============================================================================
"""Test cases for the DefectDB module

    Run this test using this command:

        python -m unittest datamanager.py

    Read more here: https://docs.python.org/3/library/unittest.html
"""
import unittest
import pandas as pd
from csvdm import DataManager


class TestDataManager(unittest.TestCase):

    # Example csv entry: 999999999999,A1,CC,Operator,Station 1,9999
    test_entry = [[
        '99999999999',      # panel_id
        'A1',               # location
        'CC',               # defect_type
        'Operator',         # cause
        'Bussing Station',  # origin
    ]]

    def setUp(self):
        # Is your working directory set to the project root and create 'data/testdata.csv' ?
        self.baseline = pd.read_csv('data/testdata.csv')
        self.db = DataManager('testdata.csv', 'data', 'uid')
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
        self.assertEqual(self.db.count('MS', 'defect_type'), 4, 'There are actually 4 missed solders in the dataset')
        self.assertEqual(self.db.count('Nuclear Meltdown', 'defect_type'), 0, '0 nukes here')

    def test_insert_row(self):
        next_uid = self.db.get_next_uid()
        self.assertFalse(self.db.contains(next_uid), 'The next UID should not be there')
        self.db.insert(self.test_entry)
        self.assertEqual(self.db.row_count(), self.data_size + 1, 'Does not have added row')
        self.assertTrue(self.db.contains(next_uid), 'No rows contain added UID')

    def test_delete_row(self):
        self.db.delete(5)
        self.assertEqual(self.db.row_count(), self.data_size - 1, 'Incorrect number of rows')
        self.assertFalse(self.db.contains(5), 'UID #5 should not exist here since we deleted it')
        # self.assertFalse(5 in self.db._data.uid)

    def test_delete_rows_where(self):
        self.db.delete_where('MS', 'defect_type')  # There are 4 of these in our dataset
        self.assertEqual(self.db.row_count(), self.data_size - 4, 'Incorrect number of rows')

    def test_delete_row_where_uid_doesnt_exist(self):
        self.db.delete(self.data_size + 1)  # Add one more than exist in the set
        self.assertEqual(self.db.row_count(), self.data_size, 'Nothing deleted, should have same number of rows')

    @unittest.skip
    def test_save(self):
        self.fail()


if __name__ == '__main__':
    unittest.main()
