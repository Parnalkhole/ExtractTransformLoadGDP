# Importing required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
from datetime import datetime 

# Function to log progress at various stages of execution
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Function to extract data from the webpage and save it to a DataFrame
def extract(url, table_attribs):
    ''' This function extracts the required information from the website and returns it as a DataFrame. '''
    try:
        page = requests.get(url).text
        soup = BeautifulSoup(page, 'html.parser')
        df = pd.DataFrame(columns=table_attribs)
        tables = soup.find_all('tbody')
        rows = tables[2].find_all('tr')
        
        for row in rows:
            col = row.find_all('td')
            if len(col) != 0:
                if col[0].find('a') is not None and '—' not in col[2].text.strip():
                    data_dict = {"Country": col[0].a.contents[0],
                                 "GDP_USD_millions": col[2].text.strip()}
                    df = pd.concat([df, pd.DataFrame(data_dict, index=[0])], ignore_index=True)
        return df
    except Exception as e:
        log_progress(f"Error in data extraction: {e}")
        raise

# Function to transform the extracted data
def transform(df):
    ''' This function converts the GDP information from Currency format to float value, 
        transforms the information of GDP from USD (Millions) to USD (Billions) rounding to 2 decimal places. '''
    try:
        # Remove commas, convert to float, and convert from millions to billions
        df["GDP_USD_billions"] = df["GDP_USD_millions"].replace({',': ''}, regex=True).astype(float) / 1000
        df = df.drop(columns=["GDP_USD_millions"])  # Drop the original "GDP_USD_millions" column
        df["GDP_USD_billions"] = df["GDP_USD_billions"].round(2)  # Round to 2 decimal places
        return df
    except Exception as e:
        log_progress(f"Error in transformation: {e}")
        raise

# Function to load the DataFrame to a CSV file
def load_to_csv(df, csv_path):
    ''' This function saves the final DataFrame as a CSV file in the provided path. '''
    try:
        df.to_csv(csv_path, index=False)
        log_progress(f"Data saved to CSV file: {csv_path}")
    except Exception as e:
        log_progress(f"Error in saving to CSV: {e}")
        raise

# Function to load the DataFrame to an SQLite database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final DataFrame to a database table with the provided name. '''
    try:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
        log_progress(f"Data loaded to Database as table: {table_name}")
    except Exception as e:
        log_progress(f"Error in saving to Database: {e}")
        raise

# Function to run a SQL query and print the result
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and prints the output. '''
    try:
        query_output = pd.read_sql(query_statement, sql_connection)
        print(query_output)
    except Exception as e:
        log_progress(f"Error in running query: {e}")
        raise

# Main ETL process
def main():
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    table_attribs = ["Country", "GDP_USD_millions"]
    db_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'
    csv_path = './Countries_by_GDP.csv'
    
    log_progress('Preliminaries complete. Initiating ETL process')

    # Extract the data
    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')

    # Transform the data
    df = transform(df)
    log_progress('Data transformation complete. Initiating loading process')

    # Load to CSV
    load_to_csv(df, csv_path)
    
    # Connect to the database and load data
    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL Connection initiated.')
    
    load_to_db(df, sql_connection, table_name)
    
    # Run a query to filter data
    query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query_statement, sql_connection)

    log_progress('Process Complete.')
    
    sql_connection.close()

if __name__ == "__main__":
    main()
