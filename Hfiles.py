import json
import datetime
from pathlib import Path
from pprint import pprint

class Table:
    def __init__(self, name, column_families, rows = {}):
        self.name = name
        self.column_families = column_families
        self.rows = rows
        self.enabled = True  

    def disable(self):
        self.enabled = False

    def enable(self):
        self.enabled = True

    def alter(self, column_families):
        self.column_families = column_families

    def describe(self):
        print(f"Table: {self.name}")
        print(f"Column Families: {', '.join(self.column_families)}")

    def put(self, row_key, column_family, column, value):
        if column_family not in self.column_families:
            print(f"Column family {column_family} does not exist.")
            return
        if row_key not in self.rows:
            self.rows[row_key] = {}
        if column_family not in self.rows[row_key]:
            self.rows[row_key][column_family] = {}
        if column not in self.rows[row_key][column_family]:
            self.rows[row_key][column_family][column] = {}
        timestamp = datetime.datetime.now()
        self.rows[row_key][column_family][column][timestamp.strftime("%d/%m/%Y %H:%M:%S")] = value
        self.rows = dict(sorted(self.rows.items()))
        print(f"Inserted/Updated row {row_key} in table {self.name}.")

    def get(self, row_key):
        if row_key in self.rows:
            print(f"Row {row_key} in table {self.name}:")
            pprint(self.rows[row_key])
        else:
            print(f"Row {row_key} does not exist in table {self.name}.")

    def scan(self):
        print(f"Scanning table {self.name}:\nROW\t\tCOLUMN+CELL")
        for row_key, col_family_data in self.rows.items():
            for col_family, column_data in col_family_data.items():
                for column, time_data in column_data.items():
                    for time, value in time_data.items():
                        print(f"{row_key}\t\tcolumn={col_family}:{column}, timestamp={time}, value={value}")

    def delete(self, row_key, column_family, column, timestamp=None):
        if row_key in self.rows and column_family in self.rows[row_key] and column in self.rows[row_key][column_family]:
            del self.rows[row_key][column_family][column]
            self.rows = dict(sorted(self.rows.items()))
            print(f"Deleted column {column_family}:{column} from row {row_key} in table {self.name}.")
        else:
            print(f"Column {column_family}:{column} does not exist in row {row_key}.")

    def delete_all(self, row_key):
        if row_key in self.rows:
            del self.rows[row_key]
            print(f"Deleted row {row_key} from table {self.name}.")
        else:
            print(f"Row {row_key} does not exist in table {self.name}.")

    def count(self):
        return len(self.rows)


def read_hfile(file):
    f = open(file)
    
    name = Path(file).stem
    data = json.load(f)
    column_family = []
    
    for cf in data.values():
        column_family = cf.keys()
        break
    
    table = Table(name, column_family, data)
    
    return table

def write_hfile(table: Table):
    f = open("regions/" + table.name + ".hfile", 'w')
    json.dump(table.rows, f)
    
    