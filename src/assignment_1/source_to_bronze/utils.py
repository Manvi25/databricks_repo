# Databricks notebook source
from pyspark.sql.functions import udf, current_date,col, sum, count,avg

# COMMAND ----------

def read_csv(path):
    read_csv = spark.read.csv(path , header=True , inferSchema=True)
    return read_csv
     

# COMMAND ----------

def write_csv( df , path):
     df.write.format('csv').save(path)

# COMMAND ----------

def read_custom_schema(path , custom_schema):
    custom_csv = spark.read.csv(path, schema = custom_schema)
    return custom_csv

# COMMAND ----------

def camel_to_snake(df):
    for cols in df.columns:
        df = df.withColumnRenamed(cols , cols.lower())
    return df

udf(camel_to_snake)

# COMMAND ----------

def current_date_df(df):
  df= df.withColumn("load_date", current_date())
  return df

# COMMAND ----------

def salary_of_each_department(df):
    salary_by_department = df.groupby("department").agg(sum("salary").alias("total_salary"))
    salary_desc = salary_by_department.orderBy(col("total_salary").desc())
    return salary_desc

# COMMAND ----------

def employee_count(df):
    employees_count = df.groupBy("department", "country").count()
    return employees_count

# COMMAND ----------

def list_the_department(df):
    dept_and_country = df.select("department", "country").distinct()
    return dept_and_country

# COMMAND ----------

def avg_age(df):
    avg_age_by_department = df.groupBy("department").agg(avg("age").alias("average_age"))
    return avg_age_by_department

# COMMAND ----------

def add_load_date(df):
    df_with_load_date = df.withColumn("at_load_date", current_date())
    return df_with_load_date