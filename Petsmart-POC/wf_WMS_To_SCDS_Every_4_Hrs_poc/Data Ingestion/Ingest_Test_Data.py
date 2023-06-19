# Databricks notebook source
pip install openpyxl;

# COMMAND ----------

import pandas as pd
tables = ["E_DEPT", "UCL_USER", "E_CONSOL_PERF_SMRY", "DAYS","SITE_PROFILE"]
# tables = ["UCL_USER"]
file_path = "/Workspace/Shared/WMS-Petsmart-POC/Workflow1/Data Ingestion/Test_Data.xlsx"
for table in tables:
    print(table)
    df = pd.read_excel(file_path, sheet_name=table)
    spark_df = spark.createDataFrame(df)
    if(table == "UCL_USER"):
        print(spark_df.count())
        spark_df = spark_df.dropDuplicates(["USER_NAME"])
        print(spark_df.count())
        spark_df.createOrReplaceTempView("Temp_" + table)
        temp_spark_df = spark.sql("""SELECT * FROM Temp_UCL_USER where user_name NOT IN (SELECT USER_NAME FROM DELTA_TRAINING.UCL_USER)""")
        temp_spark_df.display()
        print(temp_spark_df.count())
    # spark_df.createOrReplaceTempView(table)    
    spark_df.write.insertInto("DELTA_TRAINING." + table)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT count(*) FROM DELTA_TRAINING.E_DEPT;
# MAGIC -- SELECT count(*) FROM DELTA_TRAINING.UCL_USER;
# MAGIC -- SELECT count(*) FROM DELTA_TRAINING.E_CONSOL_PERF_SMRY;
# MAGIC -- SELECT count(*) FROM DELTA_TRAINING.DAYS;
# MAGIC -- SELECT count(*) FROM DELTA_TRAINING.SITE_PROFILE;
# MAGIC -- SELECT count(*) FROM delta_training.wm_ucl_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.E_DEPT
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.UCL_USER
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.E_CONSOL_PERF_SMRY
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.DAYS
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.SITE_PROFILE
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_E_CONSOL_PERF_SMRY
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_E_CONSOL_PERF_SMRY_PRE
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_UCL_USER
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_UCL_USER_PRE
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_E_DEPT
# MAGIC -- TRUNCATE TABLE DELTA_TRAINING.WM_E_DEPT_PRE

# COMMAND ----------

