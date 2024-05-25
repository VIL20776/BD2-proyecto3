from matplotlib import table
from Hfiles import Table, read_hfile, write_hfile
from pprint import pprint
import time

class HBaseSimulator:
    def __init__(self):
        self.tables = {}
        
    def load_table(self, table: Table):
        if table.name in self.tables:
            print(f"Table {table.name} already exists.")
        else:
            self.tables[table.name] = table
            print(f"Table {table.name} loaded successfully.")
    
    def save_table(self, table_name):
        if table_name not in self.tables:
            print(f"Table {table_name} does not exists.")
        else:
            write_hfile(self.tables[table_name])
            print(f"Table {table_name} saved successfully.")

    # DDL (Lenguaje de definición de datos)
    def create_table(self, table_name, column_families):
        if table_name in self.tables:
            print(f"Table {table_name} already exists.")
        else:
            self.tables[table_name] = Table(table_name, column_families)
            print(f"Table {table_name} created successfully.")

    def list_tables(self):
        if not self.tables:
            print("No tables available.")
        else:
            for table in self.tables.keys():
                print(table)

    def disable_table(self, table_name):
        if table_name in self.tables:
            self.tables[table_name].disable()
            print(f"Table {table_name} disabled.")
        else:
            print(f"Table {table_name} does not exist.")

    def is_enabled(self, table_name):
        if table_name in self.tables:
            if self.tables[table_name].enabled:
                print(f"Table {table_name} is enabled.")
                return True
            else:
                print(f"Table {table_name} is disabled.")
                return False
        else:
            print(f"Table {table_name} does not exist.")
            return False

    def alter_table(self, table_name, column_families):
        if table_name in self.tables:
            if not self.is_enabled(table_name):
                print(f"Table {table_name} is disabled. Operation not permitted.")
                return
            self.tables[table_name].alter(column_families)
            print(f"Table {table_name} altered successfully.")
        else:
            print(f"Table {table_name} does not exist.")

    def drop_table(self, table_name):
        if table_name in self.tables:
            del self.tables[table_name]
            print(f"Table {table_name} dropped successfully.")
        else:
            print(f"Table {table_name} does not exist.")

    def drop_all_tables(self):
        self.tables.clear()
        print("All tables dropped successfully.")

    def describe_table(self, table_name):
        if table_name in self.tables:
            self.tables[table_name].describe()
        else:
            print(f"Table {table_name} does not exist.")

    # DML (Lenguaje de manipulación de datos)
    def put(self, table_name, row_key, column_family, column, value):
        if table_name in self.tables:
            self.tables[table_name].put(row_key, column_family, column, value)
        else:
            print(f"Table {table_name} does not exist.")

    def get(self, table_name, row_key):
        if table_name in self.tables:
            result = self.tables[table_name].get(row_key)
            pprint(result)
        else:
            print(f"Table {table_name} does not exist.")

    def scan(self, table_name):
        if table_name in self.tables:
            self.tables[table_name].scan()
        else:
            print(f"Table {table_name} does not exist.")

    def delete(self, table_name, row_key, column_family, column):
        if table_name in self.tables:
            self.tables[table_name].delete(row_key, column_family, column)
        else:
            print(f"Table {table_name} does not exist.")

    def delete_all(self, table_name, row_key):
        if table_name in self.tables:
            self.tables[table_name].delete_all(row_key)
        else:
            print(f"Table {table_name} does not exist.")

    def count(self, table_name):
        if table_name in self.tables:
            count = self.tables[table_name].count()
            print(f"Table {table_name} has {count} rows.")
        else:
            print(f"Table {table_name} does not exist.")

    def truncate(self, table_name):
        if table_name in self.tables:
            start_time = time.time()
            print(f"Truncating '{table_name}' table (it may take a while):")
            print(f" - Disabling table {table_name}...")
            self.tables[table_name].disable()
            print(f" - Truncating table {table_name}...")
            column_families = self.tables[table_name].column_families
            del self.tables[table_name]
            print(f" - Creating table {table_name}...")
            self.tables[table_name] = Table(table_name, column_families)
            print(f" - Enabling table {table_name}...")
            end_time = time.time()
            print(f"Table {table_name} truncated successfully.")
            print(f"0 row(s) in {end_time - start_time:.4f} seconds")
        else:
            print(f"Table {table_name} does not exist.")


def main():
    simulator = HBaseSimulator()
    while True:
        print("\n1. DDL (Lenguaje de definición de datos)")
        print("2. DML (Lenguaje de manipulación de datos)")
        print("3. Cargar/Guardar archivos")
        print("4. Salir")
        choice = input("Seleccione una opción: ")
        if choice == "1":
            while True:
                print("\nDDL (Lenguaje de definición de datos)")
                print("1. Create")
                print("2. List")
                print("3. Disable")
                print("4. Is_enabled")
                print("5. Alter")
                print("6. Drop")
                print("7. Drop All")
                print("8. Describe")
                print("9. Volver al menú principal")
                ddl_choice = input("Seleccione una opción de DDL: ")
                if ddl_choice == "1":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    column_families = input("Ingrese las column families separadas por comas: ").split(",")
                    simulator.create_table(table_name, column_families)
                elif ddl_choice == "2":
                    simulator.list_tables()
                elif ddl_choice == "3":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.disable_table(table_name)
                elif ddl_choice == "4":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.is_enabled(table_name)
                elif ddl_choice == "5":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    column_families = input("Ingrese las nuevas column families separadas por comas: ").split(",")
                    simulator.alter_table(table_name, column_families)
                elif ddl_choice == "6":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.drop_table(table_name)
                elif ddl_choice == "7":
                    simulator.drop_all_tables()
                elif ddl_choice == "8":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.describe_table(table_name)
                elif ddl_choice == "9":
                    break
                else:
                    print("Opción no válida.")
        elif choice == "2":
            while True:
                print("\nDML (Lenguaje de manipulación de datos)")
                print("1. Put")
                print("2. Get")
                print("3. Scan")
                print("4. Delete")
                print("5. Deleteall")
                print("6. Count")
                print("7. Truncate")
                print("8. Volver al menú principal")
                dml_choice = input("Seleccione una opción de DML: ")
                if dml_choice == "1":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    row_key = input("Ingrese el row key: ")
                    column_family = input("Ingrese la column family: ")
                    column = input("Ingrese la columna: ")
                    value = input("Ingrese el valor: ")
                    simulator.put(table_name, row_key, column_family, column, value)
                elif dml_choice == "2":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    row_key = input("Ingrese el row key: ")
                    simulator.get(table_name, row_key)
                elif dml_choice == "3":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.scan(table_name)
                elif dml_choice == "4":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    row_key = input("Ingrese el row key: ")
                    column_family = input("Ingrese la column family: ")
                    column = input("Ingrese la columna: ")
                    simulator.delete(table_name, row_key, column_family, column)
                elif dml_choice == "5":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    row_key = input("Ingrese el row key: ")
                    simulator.delete_all(table_name, row_key)
                elif dml_choice == "6":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.count(table_name)
                elif dml_choice == "7":
                    table_name = input("Ingrese el nombre de la tabla: ")
                    simulator.truncate(table_name)
                elif dml_choice == "8":
                    break
                else:
                    print("Opción no válida.")
        elif choice == "3":
            while True:
                print("\nCargar/Guardar archivos")
                print("1. Cargar")
                print("2. Guardar")
                print("3. Regresar")
                sl_choice = input("Seleccione una opcion: ")
                if sl_choice == '1':
                    file_path = input("\nEscriba la ruta del archivo: ")
                    table = read_hfile(file_path)
                    simulator.load_table(table)
                elif sl_choice == '2':
                    table_name = input("\nEscriba el nombre de la tabla a guardar: ")
                    simulator.save_table(table_name)
                elif sl_choice == '3':
                    break
                else:   
                    print("Opción no válida.")
            
        elif choice == "4":
            break
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    main()
