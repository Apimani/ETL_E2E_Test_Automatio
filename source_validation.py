import pandas as pd
#from tabulate import tabulate

# Read the CSV file
source = pd.read_csv("employee.csv", sep=",", engine='python')

# Initialize a list to store the summary
test_summary = []

# Test Case 1: Display column names
columns = list(source.columns)
test_summary.append({
    "Test Case": "Test Case 1",
    "Description": "Column names in the source file",
    "Result": ", ".join(columns)
})


# Test Case 2: Display the shape (rows x columns)
shape = source.shape
test_summary.append({
    "Test Case": "Test Case 2",
    "Description": "Rows x columns in the source file",
    "Result": f"{shape[0]} rows x {shape[1]} columns"
})

# Test Case 3: Number of non-NA/null entries for each column
non_null_counts = source.count()
test_summary.append({
    "Test Case": "Test Case 3",
    "Description": "Number of non-NA/null entries for each column",
    "Result": non_null_counts.to_dict()
})

# Test Case 4: Count duplicate records
duplicate_count = source.duplicated().sum()
test_summary.append({
    "Test Case": "Test Case 4",
    "Description": "Count of duplicate records in the source file",
    "Result": duplicate_count
})

# Test Case 5: Save duplicate records to a new file
dupes = source[source.duplicated()]
dupes.to_csv("duplicated.csv", index=False)
test_summary.append({
    "Test Case": "Test Case 5",
    "Description": "Duplicate records saved to 'duplicated.csv'",
    "Result": f"{len(dupes)} duplicate records saved"
})

# Test Case 6: Check for NULL values in the 'department' column
null_department = source[source['department'].isnull()]
test_summary.append({
    "Test Case": "Test Case 6",
    "Description": "NULL values in the 'department' column",
    "Result": f"{len(null_department)} null rows found"
})

# Test Case 6_a: Check for NULL values in the 'salary' column
null_salary = source[source['salary'].isnull()]
test_summary.append({
    "Test Case": "Test Case 6_a",
    "Description": "NULL values in the 'salary' column",
    "Result": f"{len(null_salary)} null rows found"
})
'''
# Test Case 7: Unique values in the 'email' column
unique_emails = len(source['email'].unique())
test_summary.append({
    "Test Case": "Test Case 7",
    "Description": "Count of unique values in the 'email' column",
    "Result": unique_emails
})
'''
# Test Case 8: Unique values in the 'first_name' column
unique_names = len(source['first_name'].unique())
test_summary.append({
    "Test Case": "Test Case 8",
    "Description": "Count of unique values in the 'first_name' column",
    "Result": unique_names
})

# Test Case 9: Unique values in the 'department' column
unique_departments = len(source['department'].unique())
test_summary.append({
    "Test Case": "Test Case 9",
    "Description": "Count of unique values in the 'department' column",
    "Result": unique_departments
})

# Test Case 10: Unique values in the 'salary' column
unique_salaries = len(source['salary'].unique())
test_summary.append({
    "Test Case": "Test Case 10",
    "Description": "Count of unique values in the 'salary' column",
    "Result": unique_salaries
})

# Test Case 11: Display the first 5 rows
sample_top = source.head().to_dict()
test_summary.append({
    "Test Case": "Test Case 11",
    "Description": "Top 5 rows of the dataset",
    "Result": sample_top
})

# Test Case 12: Display the last 5 rows
sample_bottom = source.tail().to_dict()
test_summary.append({
    "Test Case": "Test Case 12",
    "Description": "Bottom 5 rows of the dataset",
    "Result": sample_bottom
})

# Generate summary report
report_file = "test_summary_report.csv"
summary_df = pd.DataFrame(test_summary)
summary_df.to_csv(report_file, index=False)

print(f"TEST COMPLETED. Summary saved to {report_file}.")
