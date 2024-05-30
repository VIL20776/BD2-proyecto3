from os import listdir, remove
from Hfiles import Table, read_hfile, write_hfile
from pprint import pprint
import time

class HBaseSimulator:
    def __init__(self):
        self.tables: Table = {}

        region_files = listdir("regions/3")
        if region_files:
            for f in region_files:
                table = read_hfile(f"regions/{f}")
                self.tables[table.name] = table                   
        
    def load_table(self, table: Table):
        if table.name in self.tables:
            print(f"Table {table.name} already exists.")
        else:
            self.tables[table.name] = table
            print(f"Table {table.name} loaded successfully.")

    def save_table(self, table_name):
        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")
        else:
            write_hfile(self.tables[table_name])
            print(f"Table {table_name} saved successfully.")

    # DDL (Lenguaje de definición de datos)
    def create_table(self, table_name, column_families):
        if table_name in self.tables:
            print(f"Table {table_name} already exists.")
        else:
            self.tables[table_name] = Table(table_name, column_families)
            write_hfile(self.tables[table_name]) 
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
            remove(f"regions/{table_name}.hfile")
            print(f"Table {table_name} dropped successfully.")
        else:
            print(f"Table {table_name} does not exist.")

    def drop_all_tables(self):
        for t in self.tables:
            remove(f"regions/{t}.hfile")
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
            write_hfile(self.tables[table_name])
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
            write_hfile(self.tables[table_name])
        else:
            print(f"Table {table_name} does not exist.")

    def delete_all(self, table_name, row_key):
        if table_name in self.tables:
            self.tables[table_name].delete_all(row_key)
            write_hfile(self.tables[table_name])
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
            write_hfile(self.tables[table_name])
            print(f"Table {table_name} truncated successfully.")
            print(f"0 row(s) in {end_time - start_time:.4f} seconds")
        else:
            print(f"Table {table_name} does not exist.")

    # Extra functions
    def insert_many(self, table_name, rows):
        if table_name in self.tables:
            for row in rows:
                row_key, column_family, column, value = row
                self.tables[table_name].put(row_key, column_family, column, value)
            print(f"Inserted {len(rows)} rows into {table_name} successfully.")
            write_hfile(self.tables[table_name])
        else:
            print(f"Table {table_name} does not exist.")

    def update_many(self, table_name, rows):
        if table_name in self.tables:
            for row in rows:
                row_key, column_family, column, value = row
                self.tables[table_name].put(row_key, column_family, column, value)
            print(f"Updated {len(rows)} rows in {table_name} successfully.")
            write_hfile(self.tables[table_name])
        else:
            print(f"Table {table_name} does not exist.")

    def index_table(self, table_name):
        if table_name in self.tables:
            # Placeholder for indexing logic
            print(f"Indexing table {table_name}...")
            # Example logic: create an index for quick access
            indexed_table = {}
            for row_key, row_data in self.tables[table_name].rows.items():
                for column_family, columns in row_data.items():
                    for column, value in columns.items():
                        if column not in indexed_table:
                            indexed_table[column] = []
                        indexed_table[column].append((row_key, value))
            self.tables[table_name].index = indexed_table
            print(f"Table {table_name} indexed successfully.")
        else:
            print(f"Table {table_name} does not exist.")

def main():
    simulator = HBaseSimulator()
    
    def input_with_prompt(prompt):
        return input(f"\n{prompt}")

    def handle_ddl(simulator):
        while True:
            print("\nDDL (Lenguaje de definición de datos)")
            ddl_options = [
                "1. Create",
                "2. List",
                "3. Disable",
                "4. Is_enabled",
                "5. Alter",
                "6. Drop",
                "7. Drop All",
                "8. Describe",
                "9. Volver al menú principal"
            ]
            print("\n".join(ddl_options))
            ddl_choice = input("Seleccione una opción de DDL: ")
            if ddl_choice == "1":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                column_families = input_with_prompt("Ingrese las column families separadas por comas: ").split(",")
                simulator.create_table(table_name, column_families)
            elif ddl_choice == "2":
                simulator.list_tables()
            elif ddl_choice == "3":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.disable_table(table_name)
            elif ddl_choice == "4":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.is_enabled(table_name)
            elif ddl_choice == "5":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                column_families = input_with_prompt("Ingrese las nuevas column families separadas por comas: ").split(",")
                simulator.alter_table(table_name, column_families)
            elif ddl_choice == "6":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.drop_table(table_name)
            elif ddl_choice == "7":
                simulator.drop_all_tables()
            elif ddl_choice == "8":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.describe_table(table_name)
            elif ddl_choice == "9":
                break
            else:
                print("Opción no válida.")

    def handle_dml(simulator):
        while True:
            print("\nDML (Lenguaje de manipulación de datos)")
            dml_options = [
                "1. Put",
                "2. Get",
                "3. Scan",
                "4. Delete",
                "5. Deleteall",
                "6. Count",
                "7. Truncate",
                "8. Volver al menú principal"
            ]
            print("\n".join(dml_options))
            dml_choice = input("Seleccione una opción de DML: ")
            if dml_choice == "1":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                row_key = input_with_prompt("Ingrese el row key: ")
                column_family = input_with_prompt("Ingrese la column family: ")
                column = input_with_prompt("Ingrese la columna: ")
                value = input_with_prompt("Ingrese el valor: ")
                simulator.put(table_name, row_key, column_family, column, value)
            elif dml_choice == "2":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                row_key = input_with_prompt("Ingrese el row key: ")
                simulator.get(table_name, row_key)
            elif dml_choice == "3":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.scan(table_name)
            elif dml_choice == "4":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                row_key = input_with_prompt("Ingrese el row key: ")
                column_family = input_with_prompt("Ingrese la column family: ")
                column = input_with_prompt("Ingrese la columna: ")
                simulator.delete(table_name, row_key, column_family, column)
            elif dml_choice == "5":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                row_key = input_with_prompt("Ingrese el row key: ")
                simulator.delete_all(table_name, row_key)
            elif dml_choice == "6":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.count(table_name)
            elif dml_choice == "7":
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.truncate(table_name)
            elif dml_choice == "8":
                break
            else:
                print("Opción no válida.")

    def handle_load_save(simulator):
        while True:
            print("\nCargar/Guardar archivos")
            sl_options = [
                "1. Cargar",
                "2. Guardar",
                "3. Regresar"
            ]
            print("\n".join(sl_options))
            sl_choice = input("Seleccione una opcion: ")
            if sl_choice == '1':
                file_path = input_with_prompt("Escriba la ruta del archivo: ")
                try:
                    table = read_hfile(file_path)
                    simulator.load_table(table)
                except Exception as e:
                    print(f"Error loading file: {e}")
            elif sl_choice == '2':
                table_name = input_with_prompt("Escriba el nombre de la tabla a guardar: ")
                simulator.save_table(table_name)
            elif sl_choice == '3':
                break
            else:
                print("Opción no válida.")

    def handle_extras(simulator):
        while True:
            print("\nExtras")
            extras_options = [
                "1. Insert many",
                "2. Update many",
                "3. Indexado",
                "4. Regresar"
            ]
            print("\n".join(extras_options))
            extras_choice = input("Seleccione una opcion: ")
            if extras_choice == '1':
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                num_rows = int(input_with_prompt("Ingrese el número de filas a insertar: "))
                rows = []
                for _ in range(num_rows):
                    row_key = input_with_prompt("Ingrese el row key: ")
                    column_family = input_with_prompt("Ingrese la column family: ")
                    column = input_with_prompt("Ingrese la columna: ")
                    value = input_with_prompt("Ingrese el valor: ")
                    rows.append((row_key, column_family, column, value))
                simulator.insert_many(table_name, rows)
            elif extras_choice == '2':
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                num_rows = int(input_with_prompt("Ingrese el número de filas a actualizar: "))
                rows = []
                for _ in range(num_rows):
                    row_key = input_with_prompt("Ingrese el row key: ")
                    column_family = input_with_prompt("Ingrese la column family: ")
                    column = input_with_prompt("Ingrese la columna: ")
                    value = input_with_prompt("Ingrese el valor: ")
                    rows.append((row_key, column_family, column, value))
                simulator.update_many(table_name, rows)
            elif extras_choice == '3':
                table_name = input_with_prompt("Ingrese el nombre de la tabla: ")
                simulator.index_table(table_name)
            elif extras_choice == '4':
                break
            else:
                print("Opción no válida.")

    while True:
        print("\n1. DDL (Lenguaje de definición de datos)")
        print("2. DML (Lenguaje de manipulación de datos)")
        print("3. Cargar/Guardar archivos")
        print("4. Extras")
        print("5. Salir")
        choice = input("Seleccione una opción: ")
        if choice == "1":
            handle_ddl(simulator)
        elif choice == "2":
            handle_dml(simulator)
        elif choice == "3":
            handle_load_save(simulator)
        elif choice == "4":
            handle_extras(simulator)
        elif choice == "5":
            break
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    main()
