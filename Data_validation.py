import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
import os
from datetime import datetime

# Set up the script path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Logging configuration
logging.basicConfig(
    filename='etlprocess.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# MySQL engine configuration (ensure pymysql is installed)
mysql_engine = create_engine('mysql+pymysql://root:Tiger@localhost:3306/mpmani')

# Fetch data from the database
try:
    query = "SELECT * FROM employees_tgt"  # Replace with your table or query
    data = pd.read_sql(query, con=mysql_engine)
    logger.info("Data fetched successfully from the database.")
except Exception as e:
    logger.error(f"Error fetching data: {e}")
    sys.exit(1)

# Define validation functions
def validate_no_nulls(data, critical_columns):
    try:
        null_counts = data[critical_columns].isnull().sum()
        if null_counts.any():
            raise ValueError(f"Null value validation failed. Null counts: {null_counts.to_dict()}")
        logger.info("No null values found in critical columns.")
    except KeyError as e:
        logger.error(f"Missing critical column for null validation: {e}")
        raise

def validate_data_types(data, column_types):
    try:
        for column, expected_type in column_types.items():
            if not data[column].map(type).eq(expected_type).all():
                raise ValueError(f"Data type validation failed for column '{column}'. Expected {expected_type}.")
        logger.info("Data type validation passed.")
    except KeyError as e:
        logger.error(f"Missing column for data type validation: {e}")
        raise

def validate_no_duplicates(data, subset_columns):
    duplicates = data.duplicated(subset=subset_columns)
    if duplicates.any():
        raise ValueError(f"Duplicate validation failed. Found {duplicates.sum()} duplicate rows.")
    logger.info("No duplicates found.")

def validate_salary_range(data, min_salary, max_salary):
    if not data['salary'].between(min_salary, max_salary).all():
        raise ValueError("Salary range validation failed. Some values are out of range.")
    logger.info("Salary range validation passed.")

def validate_bonus_calculation(data):
    if not (data['bonus'] == data['salary'] * 0.10).all():
        raise ValueError("Bonus validation failed. Some values do not match 10% of the salary.")
    logger.info("Bonus calculation validation passed.")

def validate_department_names(data, valid_departments):
    try:
        invalid_departments = data[~data['department'].isin(valid_departments)]
        if not invalid_departments.empty:
            raise ValueError(f"Department validation failed. Invalid departments: {invalid_departments['department'].unique()}")
        logger.info("Department names validation passed.")
    except KeyError as e:
        logger.error(f"Missing column for department validation: {e}")
        raise

def validate_join_date_format(data, column, date_format="%Y-%m-%d"):
    try:
        pd.to_datetime(data[column], format=date_format)
        logger.info("Join date format validation passed.")
    except KeyError as e:
        logger.error(f"Missing column for join date validation: {e}")
        raise
    except Exception as e:
        raise ValueError(f"Join date format validation failed: {e}")

def validate_aggregated_metrics(data, expected_salary_sum, expected_bonus_sum, tolerance=0.05):
    actual_salary_sum = data['salary'].sum()
    actual_bonus_sum = data['bonus'].sum()
    if abs(actual_salary_sum - expected_salary_sum) > expected_salary_sum * tolerance:
        raise ValueError(f"Salary sum validation failed. Expected {expected_salary_sum}, got {actual_salary_sum}.")
    if abs(actual_bonus_sum - expected_bonus_sum) > expected_bonus_sum * tolerance:
        raise ValueError(f"Bonus sum validation failed. Expected {expected_bonus_sum}, got {actual_bonus_sum}.")
    logger.info("Aggregated metrics validation passed.")

# Combine all validations
def run_all_validations(data):
    try:
        validate_no_nulls(data, ['employee_id', 'first_name', 'salary'])
        validate_data_types(data, {
            'employee_id': int,
            'first_name': str,
            'last_name': str,
            'salary': int,
            'bonus': float
        })
        validate_no_duplicates(data, subset_columns=['employee_id'])
        validate_salary_range(data, 60000, 100000)
        validate_bonus_calculation(data)
        validate_department_names(data, valid_departments=['Engineering', 'Marketing', 'Finance'])
        validate_join_date_format(data, column='join_date')
        validate_aggregated_metrics(data, expected_salary_sum=280000, expected_bonus_sum=28000)
        logger.info("All validations passed successfully!")
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise

# Run the validation suite
run_all_validations(data)
