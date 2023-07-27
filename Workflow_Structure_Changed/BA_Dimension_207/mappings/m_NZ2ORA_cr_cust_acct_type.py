# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_cr_cust_acct_type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_cr_cust_acct_type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CR_CUST_ACCT_TYPE1_0


query_0 = f"""SELECT
  CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
  CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
  CUST_ACCT_RANK AS CUST_ACCT_RANK,
  SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  CR_CUST_ACCT_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CR_CUST_ACCT_TYPE1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1


query_1 = f"""SELECT
  CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
  CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
  CUST_ACCT_RANK AS CUST_ACCT_RANK,
  SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CR_CUST_ACCT_TYPE1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, EXP_Transform_2


query_2 = f"""SELECT
  CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
  CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
  CUST_ACCT_RANK AS CUST_ACCT_RANK,
  IFF(
    SALES_CUST_CAPTURE_CD = 'L',
    3,
    IFF(
      SALES_CUST_CAPTURE_CD = 'P',
      2,
      IFF(SALES_CUST_CAPTURE_CD = 'E', 2, 1)
    )
  ) AS CUST_CAPTURE_RANK,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_CR_CUST_ACCT_TYPE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Transform_2")

# COMMAND ----------
# DBTITLE 1, CR_CUST_ACCT_TYPE


spark.sql("""INSERT INTO
  CR_CUST_ACCT_TYPE
SELECT
  CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
  CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
  CUST_ACCT_RANK AS CUST_ACCT_RANK,
  CUST_CAPTURE_RANK AS CUST_CAPTURE_RANK
FROM
  EXP_Transform_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_cr_cust_acct_type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_cr_cust_acct_type", mainWorkflowId, parentName)
