# csvdb python 3 module

Getting to grips with the pandas DataFrame.

## DefectDb Class

The Defects database is a DAL class based on pandas.DataFrame which persist data in a CSV file.

  1. First column is the panels ID.
  2. Panel ID can exist in more than one entry
  
## Usage

  $ git clone https://github.com/brianteachman/csvdb
  $ cd csvdb

Run example main:

  $ python main.py
 
Run test:
 
  $ python -m unittest test_defect_db.py
