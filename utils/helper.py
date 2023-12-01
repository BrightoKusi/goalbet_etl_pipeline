import sqlalchemy as sa
import pandas as pd


def transform_data(*files):
    """
    Read CSV files and extract specific columns into DataFrames.

    Parameters:
    *files (str): Variable number of file paths representing CSV files.

    Returns:
    list: A list containing DataFrames. Each DataFrame contains the first 7 columns
    extracted from the corresponding CSV file. The order of DataFrames in the list
    matches the order of provided file paths. If an error occurs during file reading
    or DataFrame creation, an empty list is returned.

    Example:
    # Read 'file1.csv' and 'file2.csv', extract first 7 columns into DataFrames
    files = ['file1.csv', 'file2.csv']
    result = transform_data(*files)
    """
    dataframes = []
    try:
        for file in files:
            df = pd.read_csv(file)
            df = df.iloc[:, 0:7]
            dataframes.append(df)
    except Exception as e:
        print('Error occurred:', e)
    return dataframes



def send_to_postgresql(result, table_names, db_user, db_password, db_host, db_database):
    """
    Send DataFrames to PostgreSQL tables.

    Parameters:
    result (list): A list containing Pandas DataFrames to be inserted into PostgreSQL tables.
    table_names (list): A list of strings representing the table names corresponding to the DataFrames.
    db_user (str): The username for PostgreSQL database connection.
    db_password (str): The password for PostgreSQL database connection.
    db_host (str): The host address of the PostgreSQL database.
    db_database (str): The name of the PostgreSQL database.

    Returns:
    None: The function doesn't return any value. It sends DataFrames to PostgreSQL tables.
    If an error occurs during the database connection or data insertion, an error message is printed.

    Example:
    # Send DataFrames to respective PostgreSQL tables
    data_frames = [df1, df2]  # List of DataFrames
    tables = ['table1', 'table2']  # Corresponding table names
    user = 'username'
    password = 'password'
    host = 'localhost'
    database_name = 'my_database'
    send_to_postgresql(data_frames, tables, user, password, host, database_name)
    """
    try:
        conn = sa.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_database}')
        for df, table_name in zip(result, table_names):
            df.to_sql(table_name, conn, if_exists='append', index=False)
    except Exception as e:
        print('Error occurred:', e)
