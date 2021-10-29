# Getting to grips with the pandas DataFrame as a CSV repository.

CSV DataManager (csvdm) Python 3 package.

## DataManager Class

The DataManager is a DAL class based on pandas.DataFrame which persist data in a CSV file.

Constraints:

  1. First column is a non-unique entity ID.
  2. The entity ID can exist in more than one entry.
  
## Setup

    $ git clone https://github.com/brianteachman/csvdb
    $ cd csvdb
    $ pip install -r requirements.txt

Run example main:

    $ python example.py
 
Run test:
 
    $ python -m unittest tests/datamanager.py
