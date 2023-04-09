import pandas as pd
import psycopg2
import time

def create_table(conn, table_name, columns):
    """
    Creates a new table in the specified database.

    conn: psycopg2.extensions.connection
        A connection object to the database.
    table_name: str
        The name of the table to be created.
    columns: dict
        A dictionary of column names and their data types.

    Returns:
    -------
    None
    """
    # create a cursor to execute SQL commands
    cur = conn.cursor()
    
    # check if the table already exists
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cur.fetchone()[0]
    
    if table_exists:
        print(f"Table {table_name} already exists in the database.")
    else:
        # construct the SQL command to create the table
        column_str = ", ".join([f"{name} {data_type}" for name, data_type in columns.items()])
        sql_command = f"CREATE TABLE {table_name} ({column_str})"
        
        # execute the SQL command to create the table
        cur.execute(sql_command)
        conn.commit()
        print(f"Table {table_name} created successfully.")
    
    # close the cursor
    cur.close()

# connect to the database
conn = psycopg2.connect(
    host="localhost",
    database="Store_Monitoring_Database",
    user="etl",
    password="demopass"
)

# define the table names for the database tables
store_status_table = "store_monitor_storestatus"
business_hours_table = "store_monitor_businesshours"
store_timezone_table = "store_monitor_storetimezone"

# define the columns and data types for each table
store_status_columns = {
    "store_id": "VARCHAR(50)",
    "status": "VARCHAR(10)",
    "timestamp_utc": "TIMESTAMP"
    
}
business_hours_columns = {
    "store_id": "VARCHAR(50)",
    "day_of_week": "INTEGER",
    "start_time_local": "TIME",
    "end_time_local": "TIME"
}
store_timezone_columns = {
    "store_id": "VARCHAR(50)",
    "timezone_str": "VARCHAR(50)"
}

# create the tables if they do not already exist
create_table(conn, store_status_table, store_status_columns)
create_table(conn, business_hours_table, business_hours_columns)
create_table(conn, store_timezone_table, store_timezone_columns)

# define a function to load the CSV data into the database
def load_csv_to_database(csv_file, table_name):
    # read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # convert the DataFrame to a list of tuples
    data = df.values.tolist()
    
    # create a cursor to execute SQL commands
    cur = conn.cursor()
    
    # execute a SQL command to delete all rows from the table
    cur.execute(f"DELETE FROM {table_name}")
    
    # execute a SQL command to insert the data into the table
    cur.executemany(f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(data[0]))})", data)
    
    # commit the changes to the database
    conn.commit()
    
    # close the cursor
    cur.close()

# define the file paths for the CSV files
store_status_file = "Database_Files/store_status.csv"
business_hours_file = "Database_Files/business_hours.csv"
store_timezone_file = "Database_Files/store_timezone.csv"

# load the initial data into the database
load_csv_to_database(store_status_file, store_status_table)
load_csv_to_database(business_hours_file, business_hours_table)
load_csv_to_database(store_timezone_file, store_timezone_table)


# define a function to update the database every hour
def update_database():
    while True:
        # wait for an hour
        time.sleep(3600)
        
        # load the updated data from the CSV files
        updated_store_status = pd.read_csv(store_status_file)
        updated_business_hours = pd.read_csv(business_hours_file)
        updated_store_timezone = pd.read_csv(store_timezone_file)
        
        # check for changes in the store status data
        existing_store_status = pd.read_sql_query(f"SELECT * FROM {store_status_table}", conn)
        if not updated_store_status.equals(existing_store_status):
            load_csv_to_database(store_status_file, store_status_table)
        
        # check for changes in the business hours data
        existing_business_hours = pd.read_sql_query(f"SELECT * FROM {business_hours_table}", conn)
        if not updated_business_hours.equals(existing_business_hours):
            load_csv_to_database(business_hours_file, business_hours_table)
        
        # check for changes in the store timezone data
        existing_store_timezone = pd.read_sql_query(f"SELECT * FROM {store_timezone_table}", conn)
        if not updated_store_timezone.equals(existing_store_timezone):
            load_csv_to_database(store_timezone_file, store_timezone_table)

# start the update process
update_database()