
### This program will give ouput by considering only by single primary key 


import pymssql
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import json
import random

# Establish SQL Server connections
conn_actual = pymssql.connect("10.158.3.37", "CDC_DMS_User", "xc21kj89ik!", "Development_Market")
SQL_cursor_actual = conn_actual.cursor()

conn_archive = pymssql.connect("10.158.3.37", "CDC_DMS_User", "xc21kj89ik!", "Development_Market_Archive")
SQL_cursor_archive = conn_archive.cursor()

# Establish Snowflake connection
snowflake_params = {
    "account": "cgna-canteen_data",
    "user": "RangaS01",
    "password": "qaws90Ki@#",
    "database": "CTG_DEV",
    "schema": "SIGMOID_RAW",
}

url = URL(**snowflake_params)
engine = create_engine(url)
SnowFlake_connection = engine.connect()
print('Connected to Snowflake')

with open("table_names.json") as f:
    table_names = json.load(f)["table_names"]

# Function to perform validation
def perform_validation(table_name, fiscal_week_id, is_actual_data):
    
    order_by_clause = get_primary_keys(table_name, is_actual_data)
    print(order_by_clause)


    snow_query = f"SELECT TOP 1000 * FROM CTG_DEV.SIGMOID_RAW.{table_name} WHERE FiscalWeekID = {fiscal_week_id} ORDER BY {order_by_clause}"
    print(snow_query)
    cursor = SQL_cursor_actual if is_actual_data else SQL_cursor_archive
    # Extract table name from the Snowflake table name
    parts = table_name.split('.')
    Table_Name = parts[-1]
    print('Validation Started for Table:', Table_Name)

    query = snow_query
    try:
        test_d = pd.read_sql_query(query, SnowFlake_connection)
    except Exception as e:
        print(f"Error reading data from Snowflake for table {table_name}: {str(e)}")
        test_d = pd.DataFrame()
    test_snow = pd.DataFrame(test_d)

    cursor.execute(f"SELECT TOP 1000 * FROM {table_name} WHERE FiscalWeekID = {fiscal_week_id} ORDER BY {order_by_clause}")

    sql_results = cursor.fetchall()
    test_sql = pd.DataFrame(sql_results, columns=[desc[0] for desc in cursor.description])

    # Rest of the validation process
    check = 0

    if not test_sql.empty:
        test_sql.sort_index(axis=0, inplace=True)
        test_snow.sort_index(axis=0, inplace=True)
        test_sql = test_sql.fillna(0)
        test_snow = test_snow.fillna(0)
        test_sql.columns = test_snow.columns
        test_sql = test_sql.astype(test_snow.dtypes)

        # Check if the DataFrames are equal
        if test_snow.equals(test_sql):
            check = 1
        else:
            # Check if the sorted DataFrames are equal for each column
            for col in test_sql.columns:
                test_sql_temp = test_sql[[col]].sort_values(by=col).reset_index(drop=True)
                test_snow_temp = test_snow[[col]].sort_values(by=col).reset_index(drop=True)
                if test_snow_temp.equals(test_sql_temp):
                    check = 1
                    break
                else:
                    check = 0
    else:
        print('----> Table has 0 records\n')
        check = 2

    return table_name, fiscal_week_id, check, test_snow

# Function to get primary keys of a table
def get_primary_keys(table_name, is_actual_data):
    cursor = SQL_cursor_actual if is_actual_data else SQL_cursor_archive
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table_name}'")
    rows = cursor.fetchall()
    order_column = [row[0] for row in rows]
    order_by_clause = ", ".join([f"{col} ASC" for col in order_column])
    
    return order_by_clause

# Generate 5 random fiscal week IDs with non-zero record counts
validation_results = []

for table_name in table_names:
    max_fiscal_week_id_query = f"SELECT MAX(FiscalWeekID) FROM CTG_DEV.SIGMOID_RAW.{table_name}"
    min_fiscal_week_id_query = f"SELECT MIN(FiscalWeekID) FROM CTG_DEV.SIGMOID_RAW.{table_name}"

    max_fiscal_week_id_result = SnowFlake_connection.execute(max_fiscal_week_id_query).fetchone()
    min_fiscal_week_id_result = SnowFlake_connection.execute(min_fiscal_week_id_query).fetchone()

    max_fiscal_week_id = max_fiscal_week_id_result[0]
    min_fiscal_week_id = min_fiscal_week_id_result[0]

    print("\nTable:", table_name)
    print("Maximum Fiscal Week ID:", max_fiscal_week_id)
    print("Minimum Fiscal Week ID:", min_fiscal_week_id)
 

    random_fiscal_week_ids = [962]
    while len(random_fiscal_week_ids) == 1:
        fiscal_week_id = random.randint(min_fiscal_week_id, max_fiscal_week_id)

        if fiscal_week_id not in random_fiscal_week_ids:
            record_count_actual_query = f"SELECT COUNT(*) FROM {table_name} WHERE FiscalWeekID = {fiscal_week_id}"
            SQL_cursor_actual.execute(record_count_actual_query)
            record_count_actual = SQL_cursor_actual.fetchone()[0]
            if record_count_actual > 0:
                random_fiscal_week_ids.append(fiscal_week_id)

    for fiscal_week_id in random_fiscal_week_ids:
        record_count_actual_query = f"SELECT COUNT(*) FROM {table_name} WHERE FiscalWeekID = {fiscal_week_id}"
        SQL_cursor_actual.execute(record_count_actual_query)
        record_count_actual = SQL_cursor_actual.fetchone()[0]
        print(f"\nRecord count for FiscalWeekID {fiscal_week_id} in actual data: {record_count_actual}")

        snowflake_record_count_query = f"SELECT COUNT(*) FROM CTG_DEV.SIGMOID_RAW.{table_name} WHERE FiscalWeekID = {fiscal_week_id}"
        snowflake_cursor = SnowFlake_connection.execute(snowflake_record_count_query)
        snowflake_record_count = snowflake_cursor.fetchone()[0]

        print(f"Record count for FiscalWeekID {fiscal_week_id} in Snowflake: {snowflake_record_count}")

        if record_count_actual == snowflake_record_count:
            print(f"Record counts match for FiscalWeekID {fiscal_week_id} between actual data in SQL Server and Snowflake.")
            validation_results.append(perform_validation(table_name, fiscal_week_id, is_actual_data=True))
        else:
            record_count_archival_query = f"SELECT COUNT(*) FROM {table_name} WHERE FiscalWeekID = {fiscal_week_id}"
            SQL_cursor_archive.execute(record_count_archival_query)
            record_count_archival = SQL_cursor_archive.fetchone()[0]
            if record_count_archival == snowflake_record_count:
                print(f"Record counts match for FiscalWeekID {fiscal_week_id} between data in SQL Server and Snowflake.")
                validation_results.append(perform_validation(table_name, fiscal_week_id, is_actual_data=False))
            elif record_count_archival == 0 and record_count_actual == 0:
                print(f"No data present for FiscalWeekID {fiscal_week_id} in both SQL Server and Snowflake. Skipping validation.")
            else:
                print(f"Record counts do not match for FiscalWeekID {fiscal_week_id} between SQL Server and Snowflake. Skipping validation.")

# Print validation results
for result in validation_results:
    table_name, fiscal_week_id, check, test_snow = result
    if check == 1:
        print(" ")
        print(f'Validation passed for Table: {table_name}, FiscalWeekID: {fiscal_week_id}')
    elif check == 2:
        print(" ")
        print(f'Validation skipped for Table: {table_name}, FiscalWeekID: {fiscal_week_id} as it has 0 records.')

    else:
        print(" ")
        print(f'\nValidation failed for Table: {table_name}, FiscalWeekID: {fiscal_week_id}')