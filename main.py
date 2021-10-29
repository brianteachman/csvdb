#!/usr/bin/env python3
# =============================================================================
# License: WTFPL
# =============================================================================
"""Getting to grips with the pandas DataFrame.

    Run this example script using this command:

        python main.py

    Read more here: https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
"""

from csvdb import DataManager

# =============================================================================
DATA_DIR = "data"
DATA_FILE = "testdata.csv"  # Assuming 'data/testdata.csv' exist
# -----------------------------------------------------------------------------
# Example csv entry: 999999999999,A1,CC,Operator,Station 1,9999
# =============================================================================

if __name__ == '__main__':
    # =========================================================================
    db = DataManager(DATA_FILE, DATA_DIR, 'uid')
    # =========================================================================
    print("Testing the DataManager on a defects database", end='\n\n')
    print(db.__class__)
    print(db.__doc__)
    # =========================================================================
    print(f'Starting dataset: {db.row_count()} rows.', end='\n\n')
    # db.info()
    db.dump()
    # =========================================================================
    uid = 5
    # db.delete_row(uid)
    col_name = 'uid'
    value = uid
    db.delete_where(value, col_name)  # It never happened
    col_name = 'cause'
    value = "Machine"
    db.delete_where(value, col_name)  # Maintenance is on it
    # =========================================================================
    print(f'\nAfter delete method called on uid={uid} and {col_name}={value}: {db.row_count()} rows left.', end='\n\n')
    db.dump()
    # =========================================================================
    print(f'\nEntry #5 and all Machine defects scrubbed. All clean. :)', end='\n\n')
    db.save()

    # Hourly reporting
    print(f"CC count\t| {db.count('CC', 'defect_type')}")
    print(f"MS count\t| {db.count('MS', 'defect_type')}")
    print(f"Nuclear Meltdown count\t| {db.count('Nuclear Meltdown', 'defect_type')}")
    print(f"Machine count\t| {db.count('Machine', 'cause')}")
    print(f"Operator count\t| {db.count('Operator', 'cause')}")
    print(f"Gremlins count\t| {db.count('Gremlins', 'cause')}")

    # test_defect = [
    #     ['99999999999', 'A1', 'CC', 'Operator', 'Station 1'],
    #     ['11111111111', 'A2', 'CC', 'Operator', 'The Pit']
    # ]
    # db.insert(test_defect)
    # print(db.contains('99999999999', 'panel_id'))
