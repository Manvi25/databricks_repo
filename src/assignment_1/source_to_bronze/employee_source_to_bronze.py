# Databricks notebook source
# MAGIC
# MAGIC %run ./utils
# MAGIC
# MAGIC

# COMMAND ----------

emp_path = 'dbfs:/FileStore/assignments/Employee_Q1.csv'
dep_path = 'dbfs:/FileStore/assignments/Department_Q1.csv'
country_path = 'dbfs:/FileStore/assignments/Country_Q1.csv'

# COMMAND ----------

employee_df = read_csv(employee_read_path)
department_df = read_csv(department_read_path)
country_df = read_csv(country_read_path)

# COMMAND ----------

employee_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/employee.csv'
department_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/department.csv'
country_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/country.csv'

# COMMAND ----------

write_csv(employee_df, '/FileStore/assignments/assign1/source_to_bronze/employee.csv')
write_csv(department_df, '/FileStore/assignments/assign1/source_to_bronze/department.csv')
write_csv(country_df, '/FileStore/assignments/assign1/source_to_bronze/country.csv')

# COMMAND ----------

