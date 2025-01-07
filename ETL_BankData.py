import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

# Set up logging
logging.basicConfig(
    filename="code_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Configurations
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
database_file = "bank.db"
db_table = "Largest_banks"
csv_file_path = "./bank.csv"
conversion_file = "./exchange_rate.csv"

def extract_data(url):
    logging.info("Starting data extraction.")
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find_all("tbody")
        rows = table[0].find_all("tr")

        banks = []
        for row in rows:
            cols = row.find_all("td")
            if cols:
                bank_name = cols[1].text.strip()
                market_cap_usd = float(cols[2].text.strip().replace(",", ""))
                banks.append({"Bank Name": bank_name, "Market Cap (USD)": market_cap_usd})

        df = pd.DataFrame(banks)
        print(df.head)
        logging.info(f"Extracted {len(df)} rows successfully.")
        return df
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def transform_data(df, conversion_file):
    logging.info("Starting data transformation.")
    try:
        conversion_rates = pd.read_csv(conversion_file)
        currencies = ["GBP", "EUR", "INR"]
        
        for currency in currencies:
            rate_row = conversion_rates.loc[conversion_rates["Currency"] == currency, "Rate"]
            if not rate_row.empty:
                rate = rate_row.values[0]
                df[f"Market Cap ({currency})"] = (df["Market Cap (USD)"] * rate).round(2)
            else:
                logging.warning(f"No conversion rate found for {currency}. Skipping.")

        # Convert USD to billions for clarity
        df["Market Cap (USD)"] = (df["Market Cap (USD)"] / 1e3).round(2)
        df.rename(columns={"Market Cap (USD)": "MC_USD_Billion"}, inplace=True)
        
        logging.info("Data transformation completed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

def load_to_csv(df, file_path):
    logging.info("Starting CSV loading.")
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to CSV file at {file_path}.")
    except Exception as e:
        logging.error(f"Error during CSV loading: {e}")
        raise

def load_to_db(df, database_file, table_name):
    logging.info("Starting database loading.")
    try:
        conn = sqlite3.connect(database_file)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"Data loaded to database table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error during database loading: {e}")
        raise

def run_queries(query_statement, connection):
    logging.info(f"Running query: {query_statement}")
    try:
        print(f"Query: {query_statement}")
        query_result = pd.read_sql_query(query_statement, connection)
        print(query_result)
        logging.info("Query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

def main():
    logging.info("ETL process initiated.")
    try:
        # Extract
        df = extract_data(url)

        # Transform
        df = transform_data(df, conversion_file)

        # Load
        load_to_csv(df, csv_file_path)
        load_to_db(df, database_file, db_table)

        # Run queries
        conn = sqlite3.connect(database_file)
        logging.info("SQL Connection established for running queries.")
        
        # Query 1: Print entire table
        run_queries("SELECT * FROM Largest_banks", conn)
        
        # Query 2: Print average market capitalization in GBP
        run_queries("SELECT AVG(`Market Cap (GBP)`) AS Avg_MC_GBP_Billion FROM Largest_banks", conn)
        
        # Query 3: Print top 5 bank names
        run_queries("SELECT `Bank Name` FROM Largest_banks LIMIT 5", conn)
        
        conn.close()
        logging.info("ETL process and queries completed successfully.")
    except Exception as e:
        logging.critical(f"ETL process failed: {e}")

if __name__ == "__main__":
    main()
