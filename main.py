# =============================================================================
# Getting to grips with the pandas DataFrame.
# =============================================================================
from csvdb import DefectDb

# =============================================================================
DATA_DIR = "data"
DATA_FILE = "datatable.csv"
# =============================================================================

if __name__ == '__main__':
    # =========================================================================
    db = DefectDb(DATA_FILE, DATA_DIR)
    # =========================================================================
    print("Testing the defects database", end='\n\n')
    print(db.__doc__)
    # =========================================================================
    print(f'Starting dataset: {db.row_count()} rows.', end='\n\n')
    # db.info()
    db.dump()
    # =========================================================================
    col_name = 'uid'
    value = 5
    # db.delete_row(value)
    db.delete_rows_where(col_name, value)
    col_name = 'cause'
    value = "Machine"
    db.delete_rows_where(col_name, value)
    # =========================================================================
    print(f'\nAfter delete method called on {col_name}={value}: {db.row_count()} rows left.', end='\n\n')
    db.dump()
    # =========================================================================
    print(f'\nEntry #5 and all Machine defects scrubbed. All clean. :)')
    # db.save_db()
