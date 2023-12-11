import pymssql
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sample import sql, snow

# Establish the SQL Server connection
# secret_key = sql.get('SECRET_KEY')
server = sql.get("servername")
user_sql = sql.get("username_sql")
password_sql = sql.get("password_sql")
database_sql = sql.get("Database_sql")

conn = pymssql.connect(server, user_sql, password_sql, database_sql)
SQL_cursor = conn.cursor()
print('connected to sql server')

account = snow.get("account")
user_snow = snow.get("user_snow")
password_snow = snow.get("password_snow")
database_snow = snow.get("database_snow")
schema_snow = snow.get("schema_snow")

url = URL(
    account=account,
    user=user_snow,
    password=password_snow,
    database=database_snow,
    schema=schema_snow
)
engine = create_engine(url)
SnowFlake_connection = engine.connect()
print('Connected to Snowflake')

Snow_tables_list = ['SALE']  # Add your table names here'INVENTORY_TRANSFER'

user_column_name = 'MODIFIEDDATETIME' #'ModifiedDateTime' 
user_date = '2020-12-25'

# Create a list to store the results of validation and data
validation_results = []

# Loop through tables and perform record count for the specified date
for table_name in Snow_tables_list:
    # Build Snowflake and SQL queries with date filtering
    snow_count_query = f"select count(*) from CTG_DEV.RAW_POC.{table_name} where DATE({user_column_name}) = '{user_date}'"
    sql_count_query = f"select count(*) from {table_name} where CAST({user_column_name} AS DATE) = '{user_date}'"

    # Execute count queries in Snowflake and SQL Server
    Snow_cursor = SnowFlake_connection.execute(snow_count_query)
    snowflake_row_count = Snow_cursor.fetchone()[0]

    SQL_cursor.execute(sql_count_query)
    sql_server_row_count = SQL_cursor.fetchone()[0]

    print(f'\nRow count for table {table_name} in Snowflake: {snowflake_row_count}')
    print(f'Row count for table {table_name} in SQL Server: {sql_server_row_count}')

    if snowflake_row_count != sql_server_row_count:
        print(f'\nRecord counts is not match for Table: {table_name}')

        break 

    # Check if record counts match
    if snowflake_row_count == sql_server_row_count:
        print('\nRecord counts match, Proceeding with validation.')
        
        # Build Snowflake and SQL queries for data retrieval
        snow_query = f"select * from CTG_DEV.RAW_POC.{table_name} where DATE({user_column_name}) = '{user_date}'" #order by COMPANYID"
        sql_query = f"select * from {table_name} where CAST({user_column_name} AS DATE) = '{user_date}'" #order by COMPANYID"

        # Extract table name from the Snowflake table name
        parts = table_name.split('.')
        Table_Name = parts[-1]

        print('Validation Started for Table: ' + Table_Name)

        # Snowflake table data for the specified date
        query = snow_query
        try:
            test_d = pd.read_sql(query, SnowFlake_connection)
        except Exception as e:
            print(f"Error reading data from Snowflake for table {table_name}: {str(e)}")
            test_d = pd.DataFrame()
        test_snow = pd.DataFrame(test_d)

        # SQL Server table data for the specified date
        SQL_cursor.execute(sql_query)

        # Fetch all rows from the cursor
        sql_results = SQL_cursor.fetchall()

        # Create a DataFrame from the SQL results
        test_sql = pd.DataFrame(sql_results, columns=[desc[0] for desc in SQL_cursor.description])

        # Handling boolean value bug of Python SQL connector
        sql_bool_col = [x for x, y in dict(test_sql.dtypes).items() if y == 'bool']
        if sql_bool_col:
            for a in sql_bool_col:
                test_sql[a] = test_sql[a].astype(int)

        # test_snow.to_csv('test_snow.csv')
        # test_sql.to_csv('test_sql.csv')

        # Validation Process for the specified date
        check = 0
        if not test_sql.empty:
            test_sql = test_sql.fillna(0)
            test_snow = test_snow.fillna(0)
            test_sql.columns = test_snow.columns  # Set column names for SQL data
            test_sql = test_sql.astype(test_snow.dtypes)
            if test_snow.equals(test_sql):
                check = 1
            else:
                list_col = test_sql.columns
                for a in list_col:
                    test_sql_temp = test_sql.sort_values([a], ascending=True).reset_index(drop=True)
                    test_snow_temp = test_snow.sort_values([a], ascending=True).reset_index(drop=True)
                    if test_snow_temp.equals(test_sql_temp):
                        check = 1
                        break
                    else:
                        check = 0
        else:
            print('----> Table has 0 records' + '\n')
            check = 2

        validation_results.append((f'{Table_Name} (Date Validation)', check, test_snow))


    else:
        print('Record counts do not match. Skipping validation for Table: ' + table_name)

    if check == 1:
        print(f'\nValidation is Done for Table: {table_name}')

        print('\nAll Validations for the Specified Date Passed.')

# Close the Snowflake connection
SnowFlake_connection.close()

# Check if any validation failed for the specified date
failed_validations = [result for _, result, _ in validation_results if result == 0]

if failed_validations:
    print('\n============= Report =============')
    print(f'Validations for the Specified Date:{user_date}')
    for table_name, result, data in validation_results:
        if result == 0:
            print(f'\nTable: {table_name}')
            print('\nValidation Failed')
            if not data.empty:
                # Find the mismatched rows
                mismatched_data = data.merge(test_sql, indicator=True, how='outer').loc[lambda x: x['_merge'] == 'right_only']

                # Save the mismatched data to a CSV file
                csv_filename = f'Mismatched_data_{table_name}_{user_date}.csv'
                mismatched_data.drop('_merge', axis=1, inplace=True)
                mismatched_data.to_csv(csv_filename, index=False)
                print(f'\nMismatched data saved to: {csv_filename}')




