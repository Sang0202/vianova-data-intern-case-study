import mysql.connector
import pandas as pd 


def fetch_data(api_url):
    """
    Fetches data from the specified API URL and returns it as a pandas DataFrame.
    Here the API URL is an exported CSV so I use directly pd.read_csv()

    Args:
        api_url (str): The URL of the API endpoint.

    Returns:
        pandas.DataFrame: The fetched data as a DataFrame.
    """
    df = pd.read_csv(api_url, sep=';')
    return df

def create_data_table(user, password, host, database):
    """
    Creates a data table in MySQL to store our data fetched.

    Args:
        user (str): The username for the MySQL connection.
        password (str): The password for the MySQL connection.
        host (str): The host address for the MySQL connection.
        database (str): The name of the MySQL database.
        query (str): The SQL query to create the table.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = conn.cursor()

    # Drop the table existing
    cursor.execute('DROP TABLE IF EXISTS populations')

    # Execute the query to create the table
    cursor.execute( '''
    CREATE TABLE IF NOT EXISTS populations(
        geoname_id INT NOT NULL PRIMARY KEY, 
        name VARCHAR(255), 
        ascii_name VARCHAR(255), 
        alternate_names VARCHAR(255), 
        feature_class VARCHAR(1), 
        feature_code VARCHAR(5), 
        country_code VARCHAR(2), 
        cou_name_en VARCHAR(255), 
        country_code_2 VARCHAR(50), 
        admin1_code VARCHAR(255), 
        admin2_code VARCHAR(255), 
        admin3_code VARCHAR(255), 
        admin4_code VARCHAR(255), 
        population INT, 
        elevation FLOAT, 
        dem INT, 
        timezone VARCHAR(50), 
        modification_date DATE, 
        label_en VARCHAR(255), 
        coordinates VARCHAR(50)
        )
    ''')
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close() 
    return print("Create data table successfully")

def import_data_to_mysql(user, password, host, database, data):
    """
    Imports data into MySQL from a pandas DataFrame.

    Args:
        user (str): The username for the MySQL connection.
        password (str): The password for the MySQL connection.
        host (str): The host address for the MySQL connection.
        database (str): The name of the MySQL database.
        data (pandas.DataFrame): The data to import into MySQL.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = conn.cursor()

    import_query = ''' INSERT INTO populations(
                        geoname_id, 
                        name, 
                        ascii_name, 
                        alternate_names, 
                        feature_class, 
                        feature_code, 
                        country_code, 
                        cou_name_en, 
                        country_code_2, 
                        admin1_code, 
                        admin2_code, 
                        admin3_code, 
                        admin4_code, 
                        population, 
                        elevation, 
                        dem, 
                        timezone, 
                        modification_date, 
                        label_en, 
                        coordinates 
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
    
    # Execute the query to import data
    cursor.executemany(import_query, data.values.tolist())
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    return print("Import data successfully")

def perform_query(user, password, host, database):
    """
    Performs a query on the 'populations' table to compute our result and saves in the 'results' table.

    Args:
        user (str): The username for the MySQL connection.
        password (str): The password for the MySQL connection.
        host (str): The host address for the MySQL connection.
        database (str): The name of the MySQL database.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = conn.cursor()

    # Drop previous 'results' table
    cursor.execute('DROP TABLE IF EXISTS results;')

    # Create a table to store the query results
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            country_name VARCHAR(255),
            country_code VARCHAR(2) NOT NULL PRIMARY KEY
        );
    ''')

    # SQL query to compute the result
    cursor.execute(f'''
        INSERT INTO results (country_name, country_code)
        SELECT cou_name_en, country_code
        FROM populations
        GROUP BY country_code
        HAVING MAX(population) <= 10000000
        ORDER BY
            CASE
                WHEN cou_name_en = '' THEN 1
                ELSE 0
            END,
            cou_name_en;
    ''')

    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    return print("Query and save succesfully")

def query_result(user, password, host, database):
    """
    Executes a query to retrieve the result from the 'results' table.

    Args:
        user (str): The username for the MySQL connection.
        password (str): The password for the MySQL connection.
        host (str): The host address for the MySQL connection.
        database (str): The name of the MySQL database.

    Returns:
        list: A list of tuples representing the query result.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = conn.cursor()

    # Execute the query 
    cursor.execute('SELECT * FROM results')

    # Fetch all rows from the result
    rows = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    return rows

def export_to_tsv(df, file_path):
    """
    Exports a DataFrame to a TSV (tab-separated values) file.

    Args:
        df (pandas.DataFrame): The DataFrame to export.
        file_path (str): The path to save the TSV file.
    """
    # Export DataFrame to TSV file
    df.to_csv(file_path, sep='\t', index=False)
    return print('Export tsv file successfully')

def main():
    """
    Main function to execute the program.
    """
    print('Starting program...')

    # API URL endpoint for dataset
    api_url = 'https://public.opendatasoft.com/api/explore/v2.0/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=false'
    # file path for result
    result_file_path = 'result.tsv'

    # Information to connect to MySQL
    user = 'root'
    password = ''
    host = 'localhost'
    database = 'vianova_de_test'

    # Fetch data from the API
    df = fetch_data(api_url)
    
    # Create 'populations' table in MySQL
    create_data_table(user, password, host, database)

    # Prepare data for importing
    data = df.copy()
    data = data.fillna('')

    # Import data into MySQL
    print('Importing data into MySQL...')
    import_data_to_mysql(user, password, host, database, data)

    # Perform the query and save results
    print('Performing query...')
    perform_query(user, password, host, database)
    print('Query execution completed.')

    # Retrieve query result
    result = query_result(user, password, host, database)

    # Convert result to DataFrame and export to TSV file
    df_result = pd.DataFrame(result, columns=['country_name', 'country_code'])
    export_to_tsv(df_result, result_file_path)
    
    print('Program completed.')

if __name__ == '__main__' :
    main() 