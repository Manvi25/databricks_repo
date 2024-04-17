# Databricks notebook source
# MAGIC %run /Users/manvi.khandelwal@diggibyte.com/assignments/source_to_bronze/utils

# COMMAND ----------

employee_df= spark.read.format("delta").load("dbfs:/FileStore/assignments/questoin1/silver/emp_info/dim_employee")
display(employee_df)

# COMMAND ----------

display(salary_of_each_department(employee_df))

# COMMAND ----------

display(employee_count(employee_df))

# COMMAND ----------

display(list_the_department(employee_df))

# COMMAND ----------

display(avg_age(employee_df))

# COMMAND ----------

display(add_load_date(employee_df))

# COMMAND ----------

employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")