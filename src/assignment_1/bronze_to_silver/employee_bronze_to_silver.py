# Databricks notebook source
# MAGIC %run /Users/manvi.khandelwal@diggibyte.com/assignments/source_to_bronze/utils
# MAGIC
# MAGIC

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Read file with custom schema
custom_schema = StructType([
    StructField("emp_Id", StringType(), True),
    StructField("emp_Name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("Country", StringType(),  True),
    StructField("Salary", StringType(), True),
    StructField("Age", StringType(), True),
])



# COMMAND ----------

emp_path = 'dbfs:/FileStore/assignments/Employee_Q1.csv'
employee_df = read_custom_schema(emp_path , custom_schema)
employee_df.show()
employee_df.printSchema()

# COMMAND ----------

schema = StructType([
    StructField("dep_id", StringType(), True),
    StructField("dep_name", StringType(), True)
])

# COMMAND ----------

dep_path = 'dbfs:/FileStore/assignments/Department_Q1.csv'
department_df = read_custom_schema(dep_path , schema)
department_df.show()
department_df.printSchema()

# COMMAND ----------

schema=schema = StructType([
    StructField("country_code", IntegerType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

country_path = 'dbfs:/FileStore/assignments/Country_Q1.csv'
country_df = read_custom_schema(country_path , schema)
country_df.show()
country_df.printSchema()

# COMMAND ----------

emp_snake_case = camel_to_snake(employee_df)
display(emp_snake_case)


# COMMAND ----------

date_load= current_date_df(emp_snake_case)
display(date_load)

# COMMAND ----------

spark.sql('CREATE DATABASE emp_info')
spark.sql('use emp_info')

# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignments/questoin1/silver/emp_info/dim_employee').saveAsTable('dim_employee')