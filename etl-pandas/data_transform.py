import io
import pandas as pd    
import numpy as np
import math
from argparse import ArgumentParser
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import time

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.datetime64, AsIs)

def clean_comma_in_strings(df):
    """ Cleans commas within strings in a pandas dataframe

    Args:
        df: The pandas dataframe to clean

    Returns:
        A new pandas dataframe with commas within strings cleaned
    """
    for col in df.columns:
        if df[col].dtype == object:  # Use 'object' instead of 'np.object'
            df[col] = df[col].apply(lambda x: x.replace(",", "") if isinstance(x, str) else x)
    return df

def to_int(x):
    if x == ' ' or (isinstance(x, float) and math.isnan(x)):
        return 0
    return int(x)

def connect_db(db_params):
    """ Establishes a connection to the PostgreSQL database. """
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    return conn, cur

def close_db_connection(conn, cur):
    """ Closes the connection to the PostgreSQL database. """
    conn.commit()
    cur.close()
    conn.close()
    print("Connection to PostgreSQL database closed.")

def get_month(x):
    return str(x.month) + '-' + str(x.year)

def drop_registros_table(cur):
    """ Drops the 'registros' table from the PostgreSQL database. """
    drop_query = "DROP TABLE IF EXISTS registros;"
    cur.execute(drop_query)
    print("Table 'registros' dropped successfully.")

def create_registros_table(cur):
    """ Creates the 'registros' table in the PostgreSQL database. """
    # Drop the 'registros' table if it exists
    drop_registros_table(cur)

    table_definition = """
        CREATE TABLE IF NOT EXISTS registros (
            numero_factura VARCHAR(255),
            codigo VARCHAR(255),
            descripcion TEXT,
            cantidad INT,
            fecha_factura DATE,
            precio_unitario FLOAT,
            id_cliente INT,
            pais VARCHAR(255),
            mes VARCHAR(255)
        );
    """


    cur.execute(table_definition)
    print("Table 'registros' created successfully.")

def process_data(filename, db_params):
    """Reads a CSV file, renames columns, converts data types, creates a 'mes' column, and saves to a PostgreSQL database."""
    start_time = time.perf_counter()
    df = pd.read_csv(filename, delimiter=',', encoding='unicode_escape')

    df = df.rename(columns={'InvoiceNo': 'numero_factura',
                             'Age': 'edad',
                             'City': 'ciudad',
                             'Country': 'pais',
                             'UnitPrice': 'precio_unitario',
                             'StockCode': 'codigo',
                             'Description': 'descripcion',
                             'Quantity': 'cantidad',
                             'InvoiceDate': 'fecha_factura',
                             'CustomerID': 'id_cliente'})

    df = clean_comma_in_strings(df.copy())
    df['id_cliente'] = df['id_cliente'].apply(to_int)

    df['fecha_factura'] = pd.to_datetime(df['fecha_factura']).dt.date

    df['mes'] = df['fecha_factura'].apply(get_month)


    print(df.dtypes)    
    # Connect to PostgreSQL database
    conn, cur = connect_db(db_params)

    # Create the 'registros' table
    create_registros_table(cur)


    # Prepare data for insertion
    data = df.to_csv(index=False, header=False)
    data_io = io.StringIO(data)

    print(df.columns)
    
    # Use copy_from to insert data
    cur.copy_from(data_io, 'registros', columns=df.columns, sep=',')

    close_db_connection(conn,cur)
    end_time = time.perf_counter()
    print(f"Data from '{filename}' saved to PostgreSQL database.")
    print(f"Elapsed time: {end_time - start_time:.2f} seconds.")



if __name__ == '__main__':
    parser = ArgumentParser(description='Process a CSV file and save to PostgreSQL.')
    parser.add_argument('filename', type=str, help='The name of the CSV file to process.')
    parser.add_argument('--host', type=str, default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', type=str, required=True, help='PostgreSQL database name')
    parser.add_argument('--user', type=str, required=True, help='PostgreSQL username')
    parser.add_argument('--password', type=str, required=True, help='PostgreSQL password')

    args = parser.parse_args()

    # Creates a dictionary with the PostgreSQL connection parameters
    db_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }

    process_data(args.filename, db_params)