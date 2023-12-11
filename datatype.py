# import pymssql
# import pandas as pd

# # Replace with your SQL Server connection details
# # server = 'CanteenMarketDBTest'
# # database = 'QAS_Market'
# # username = 'CDC_DMS_User'
# # password = 'xc21kj89ik!'
# # table_name = 'Product_Catalog_Detail'  # Specify the table name you're interested in

# server = 'localhost'
# database = 'sample_db'
# username = 'sa'
# password = 'Sharu@123_r'
# table_name = 'COST'  


# # Establish a connection to the SQL Server
# conn = pymssql.connect(server=server, user=username, password=password, database=database)


# try:
#     # Query to retrieve column names, data types, NULL info, and precision for a specific table
#     query = f'''
#             SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
#             FROM INFORMATION_SCHEMA.COLUMNS
#             WHERE TABLE_NAME = '{table_name}';
#             '''

#     # Execute the query and fetch the results into a DataFrame
#     df = pd.read_sql_query(query, conn)

#     # Process the information for each column and store in a list
#     columns_info = []
#     for _, row in df.iterrows():
#         col_name = row['COLUMN_NAME']
#         data_type = row['DATA_TYPE']
#         nullable = 'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'
#         details = f'({int(row["NUMERIC_PRECISION"])}, {int(row["NUMERIC_SCALE"])})' if data_type == 'decimal' else ''
#         column_info = {
#             "Column Name": col_name,
#             "Data Type": data_type,
#             "decimal": details,
#             "Nullable": nullable
#         }
#         columns_info.append(column_info)

#     # Create a DataFrame from the column information
#     result_df = pd.DataFrame(columns_info)

#     # Define the CSV file path
#     csv_file_path = f'{table_name}.csv'

#     # Write to CSV
#     result_df.to_csv(csv_file_path, index=False)

#     print(f'Column information for table "{table_name}" saved to {csv_file_path}')

# except pymssql.Error as e:
#     print(f"An error occurred: {str(e)}")

# finally:
#     # Close the connection
#     conn.close()




# import pymssql
# import pandas as pd

# # Connection parameters for SQL Server
# server = 'CanteenVendingDBTest'
# database = 'QAS_iVend'
# username = 'CDC_DMS_User'
# password = 'xc21kj89ik!'
# table_name = 'Cost_Center_Fiscal_Calendar'  # 

# # Table and query to retrieve 10 records
# query = f"SELECT TOP 10 * FROM {table_name}"

# # Establish a connection to SQL Server
# conn = pymssql.connect(server, username, password, database)
# cursor = conn.cursor()

# # Execute the query and fetch the results into a pandas DataFrame
# cursor.execute(query)
# data = cursor.fetchall()
# columns = [desc[0] for desc in cursor.description]
# df = pd.DataFrame(data, columns=columns)

# # Close the cursor and connection
# cursor.close()
# conn.close()

# # Save the DataFrame to a CSV file
# csv_filename = f"{table_name}_10_records.csv"
# df.to_csv(csv_filename, index=False)

# print(f"10 records from '{table_name}' saved to '{csv_filename}'.")









import pymssql
import snowflake.connector

# Establish the SQL Server connection
conn_sql = pymssql.connect("localhost", "sa", "Sharu@123_r", "sample_db")
SQL_cursor = conn_sql.cursor()
print('SQL Server connected')

# Specify the table for which you want to print column names and datatypes
table_name_sql = "cost"

# Fetch the column names and datatypes for the specified table in SQL Server
sql_datatypes_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name_sql}'"
SQL_cursor.execute(sql_datatypes_query)
sql_datatypes = SQL_cursor.fetchall()

# Initialize a dictionary to store converted column names and datatypes
converted_datatypes = {}

# Convert uniqueidentifier datatype to string for columns in the specified table
for column, datatype in sql_datatypes:
    if datatype == 'uniqueidentifier':
        SQL_cursor.execute(f"SELECT CONVERT(NVARCHAR(MAX), {column}) FROM {table_name_sql}")
        converted_value = SQL_cursor.fetchone()[0]
        converted_datatypes[column] = 'varchar'

# Print the converted column names and their datatypes for SQL Server
print('Converted Column Names and Datatypes (SQL Server):')
for column, datatype in converted_datatypes.items():
    print(f'{column}: {datatype}')

# Close the SQL Server connection
conn_sql.close()

# Connect to Snowflake
conn_snowflake = snowflake.connector.connect(
    user='sharath',
    password='Sharu@123_r',
    account='hv30295.ap-south-1.aws',
    warehouse='COMPASS_WH',
    database='sample_db',
    schema='sam_schema'
)


# Specify the table for which you want to print column names and datatypes in Snowflake
table_name_snowflake = "COST"

# Fetch the column names and datatypes for the specified table in Snowflake
snowflake_datatypes_query = f"DESCRIBE TABLE {table_name_snowflake}"
snowflake_cursor = conn_snowflake.cursor()
snowflake_cursor.execute(snowflake_datatypes_query)
snowflake_datatypes = snowflake_cursor.fetchall()

# Print the column names and their datatypes for Snowflake
print('\nColumn Names and Datatypes (Snowflake):')
for row in snowflake_datatypes:
    print(f'{row[0]}: {row[1]}')

# Close the Snowflake connection
conn_snowflake.close()


















# from sqlalchemy import create_engine
# from sqlalchemy.engine.url import URL
# import pymssql
# import pandas as pd

# # Establish the SQL Server connection
# conn = pymssql.connect("localhost", "sa", "Sharu@123_r", "sample_db")
# SQL_cursor = conn.cursor()
# print('SQL Server connected')

# # Define Snowflake connection parameters
# snowflake_params = {
#     "account": "hv30295.ap-south-1.aws",
#     "user": "sharath",
#     "password": "Sharu@123_r",
#     "database": "sample_db",
#     "schema": "sam_schema",
#     "warehouse": "COMPASS_WH",
#     "role": "my_custom_role",
# }

# # Specify the table for which you want to print column names and datatypes
# table_name = "YOUR_TABLE_NAME"

# # Fetch the column names and datatypes for the specified table in SQL Server
# sql_datatypes_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
# SQL_cursor.execute(sql_datatypes_query)
# sql_datatypes = SQL_cursor.fetchall()

# # Initialize a dictionary to store converted column names and datatypes for SQL Server
# converted_datatypes_sql = {}

# # Convert uniqueidentifier datatype to string for columns in the specified table in SQL Server
# for column, datatype in sql_datatypes:
#     if datatype == 'uniqueidentifier':
#         SQL_cursor.execute(f"SELECT CONVERT(NVARCHAR(MAX), {column}) FROM {table_name}")
#         converted_value = SQL_cursor.fetchone()[0]
#         converted_datatypes_sql[column] = 'nvarchar(max)'

# # Print the converted column names and their datatypes for SQL Server
# print('Converted Column Names and Datatypes for SQL Server:')
# for column, datatype in converted_datatypes_sql.items():
#     print(f'{column}: {datatype}')

# # Create Snowflake URL
# snowflake_url = URL(
#     drivername='snowflake',
#     account=snowflake_params["account"],
#     user=snowflake_params["user"],
#     password=snowflake_params["password"],
#     database=snowflake_params["database"],
#     schema=snowflake_params["schema"],
# )

# # Create Snowflake engine
# engine_snowflake = create_engine(snowflake_url)

# # Fetch the column names and datatypes for the specified table in Snowflake
# # Replace 'YOUR_SCHEMA' with the actual schema name
# snowflake_table_name = f'YOUR_SCHEMA.{table_name}'
# snowflake_datatypes_query = f"DESCRIBE TABLE {snowflake_table_name}"

# # Assuming you have a connection to Snowflake with the variable 'engine_snowflake'
# snowflake_datatypes = engine_snowflake.execute(snowflake_datatypes_query)

# # Print the column names and their datatypes for Snowflake
# print('\nColumn Names and Datatypes for Snowflake:')
# for row in snowflake_datatypes:
#     print(f'{row["name"]}: {row["type"]}')

# # Close the SQL Server connection
# conn.close()
