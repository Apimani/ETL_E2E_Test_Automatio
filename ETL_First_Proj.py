import pandas as pd
from sqlalchemy import create_engine  # Corrected import
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Logging configuration
logging.basicConfig(
    filename='etlprocess.log',  # Log file path
    filemode='a',  # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Logging level
)
logger = logging.getLogger(__name__)

# MySQL engine configuration
mysql_engine = create_engine('mysql+pymysql://root:Tiger@localhost:3306/mpmani')  # Ensure pymysql is installed

# Step 1: Extract Data
def extract_data():
    try:
        logging.info("Extracting data from CSV file...")
        data = pd.read_csv('employees.csv')  # Replace with your file path
        logging.info(f"Extracted {len(data)} rows.")
        return data
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

# Step 2: Transform Data
def transform_data(data):
    try:
        logging.info("Transforming data...")

        # Example Transformations:
        # 1. Capitalize department names
        data['department'] = data['department'].str.capitalize()

        # 2. Add a new column for bonus (10% of salary)
        data['bonus'] = data['salary'] * 0.10

        # 3. Filter out employees with salary less than 60,000
        data = data[data['salary'] >= 60000]

        logging.info("Transformation complete.")
        return data
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

# Step 3: Load Data
def load_data(data, engine, table_name):
    try:
        logging.info("Loading data into MySQL database...")

        # Load data into the specified table
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)

        logging.info(f"Data successfully loaded into table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

# Main ETL Process
if __name__ == "__main__":
    # File and database configurations
    input_csv = "employees.csv"  # Replace with your file path
    target_table = "employees_tgt"

    try:
        # Run ETL steps
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)
        load_data(transformed_data, mysql_engine, target_table)
        logging.info("ETL process completed successfully!")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
