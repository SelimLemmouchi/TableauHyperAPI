import shutil
import array

from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, Date, escape_string_literal, \
    HyperException

def run_delete_data_in_existing_hyper_file():

    #path_to_source_database = Path(__file__).parent / "data" / "T_ano_global.hyper"
    path_to_source_database = Path(__file__).parent / "data" / "Données_globales.hyper"
    #path_to_source_database = Path(__file__).parent / "data" / "NC_only.hyper"

    # Make a copy of the superstore example Hyper file.
    #path_to_database = Path(shutil.copy(path_to_source_database, "T_ano_global_delete.hyper")).resolve()
    path_to_database = Path(shutil.copy(path_to_source_database, "Données_globales_delete.hyper")).resolve()
    #path_to_database = Path(shutil.copy(path_to_source_database, "NC_only_delete.hyper")).resolve()

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Connect to existing Hyper file "superstore_sample_delete.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            # `execute_command` executes a SQL statement and returns the impacted row count.

            schemas = connection.catalog.get_schema_names()
            print(f" {schemas} ")
            print(f" {schemas[1]} ")

            tables = connection.catalog.get_table_names(schemas[1])
            print(f" {tables} ")
            print(f" {tables[0]} ")

            for table in tables :
                table_definition = connection.catalog.get_table_definition(name=table)
                print(f"  -> Table {table.name}: {len(table_definition.columns)} columns")
                for column in table_definition.columns:
                    collation = " " + column.collation if column.collation is not None else ""
                    print(f"    -> {column.name} {column.type}{collation}")

            #rr = "Date Export"
            rr = "DATE EXPORT"

            row_count = connection.execute_command(
                command=f"DELETE FROM {(tables[0])} "
                        f"WHERE {escape_name(rr)}  >  ('2022-07-10')")

            print("done")

if __name__ == '__main__':
    try:
        run_delete_data_in_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
