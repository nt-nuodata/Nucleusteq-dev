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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_non_discount_upc_TRUNC")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_non_discount_upc_TRUNC", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_WEEKS_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT
FROM
  WEEKS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_WEEKS_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_WEEKS_1


query_1 = f"""SELECT
  CURRENT_DATE AS WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_WEEKS_1")

# COMMAND ----------
# DBTITLE 1, FLT_NON_DISCOUNT_UPC_2


query_2 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  NULL AS UPC_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_WEEKS_1
WHERE
  FALSE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FLT_NON_DISCOUNT_UPC_2")

# COMMAND ----------
# DBTITLE 1, BIW_SCRIPT_DUMMY


spark.sql("""INSERT INTO
  BIW_SCRIPT_DUMMY
SELECT
  WEEK_DT AS DATE_TIME
FROM
  ASQ_Shortcut_To_WEEKS_1""")

# COMMAND ----------
# DBTITLE 1, NON_DISCOUNT_UPC


spark.sql("""INSERT INTO
  NON_DISCOUNT_UPC
SELECT
  UPC_ID AS UPC_ID,
  UPC_ID AS UPC_ID
FROM
  FLT_NON_DISCOUNT_UPC_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_non_discount_upc_TRUNC")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_non_discount_upc_TRUNC", mainWorkflowId, parentName)
