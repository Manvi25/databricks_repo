# Question 1:
## Objective:
The aim of this assignment is to utilize Databricks notebooks and Delta Lake to transform CSV files containing employee, department, and country data from the source layer to the gold layer.

## Steps Taken:
### Folder Organization:
Three folders were created: source_to_bronze, bronze_to_silver, and silver_to_gold.

### Notebook Creation:
Four notebooks were developed sequentially:
- Two notebooks in source_to_bronze: utils (containing common functions) and employee_source_to_bronze (acting as the driver notebook).
- One notebook in bronze_to_silver: employee_bronze_to_silver.
- One notebook in silver_to_gold: employee_silver_to_gold.

### Data Processing (employee_source_to_bronze):
- CSV files were read as DataFrames.
- Functions from the utils notebook were utilized.
- Transformed DataFrames were saved to DBFS in CSV format under /source_to_bronze/.

### Bronze Layer Transformation (employee_bronze_to_silver):
- CSV file from DBFS location source_to_bronze was read with a custom schema.
- Column names were converted from Camel case to snake case using a User Defined Function (UDF).
- A load_date column with the current date was added.
- The DataFrame was saved as a Delta table under /silver/employee_info/dim_employee/.

### Gold Layer Transformation (employee_silver_to_gold):
- Delta table from the silver layer was read.
- Various analyses were performed, including salary of each department in descending order, number of employees in each department located in each country, list of department names with corresponding country names, and average age of employees in each department.
- An at_load_date column was added to DataFrames.
- Data in /gold/employee/fact_employee was overwritten and replaced based on at_load_date.

# Question 2: API Data Processing
## Objective:
The objective is to retrieve data from a provided API, process it, and store it in Delta format in DBFS.

## Steps Taken:
### API Data Retrieval:
- Data was fetched from the API by passing parameters as page until no more data was returned.

### Data Processing:
- The DataFrame was read with a custom schema.
- It was flattened and a new column named site_address was derived from the email field.
- A load_date column with the current date was added.

### Data Storage:
- The DataFrame was saved to DBFS under /site_info/person_info in Delta format with overwrite mode.
