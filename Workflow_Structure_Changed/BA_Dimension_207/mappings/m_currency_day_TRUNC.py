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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_currency_day_TRUNC")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_currency_day_TRUNC", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CURRENCY_DAY_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  CURRENCY_ID AS CURRENCY_ID,
  DATE_RATE_START AS DATE_RATE_START,
  CURRENCY_TYPE AS CURRENCY_TYPE,
  DATE_RATE_ENDED AS DATE_RATE_ENDED,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  RATIO_TO AS RATIO_TO,
  RATIO_FROM AS RATIO_FROM,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  CURRENCY_NBR AS CURRENCY_NBR
FROM
  CURRENCY_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CURRENCY_DAY_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_CURRENCY_DAY_1


query_1 = f"""SELECT
  'Shortcut_to_CURRENCY_DAY_0' AS TABLE_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
LIMIT
  1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_CURRENCY_DAY_1")

# COMMAND ----------
# DBTITLE 1, FIL_TRUNC_2


query_2 = f"""SELECT
  TABLE_NAME AS TABLE_NAME,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_CURRENCY_DAY_1
WHERE
  FALSE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FIL_TRUNC_2")

# COMMAND ----------
# DBTITLE 1, DUMMY_TARGET


spark.sql("""INSERT INTO
  DUMMY_TARGET
SELECT
  TABLE_NAME AS COMMENT
FROM
  ASQ_Shortcut_to_CURRENCY_DAY_1""")

# COMMAND ----------
# DBTITLE 1, CURRENCY_DAY


spark.sql("""INSERT INTO
  CURRENCY_DAY
SELECT
  NULL AS DAY_DT,
  TABLE_NAME AS CURRENCY_ID,
  NULL AS DATE_RATE_START,
  NULL AS CURRENCY_TYPE,
  NULL AS DATE_RATE_ENDED,
  NULL AS EXCHANGE_RATE_PCNT,
  NULL AS RATIO_TO,
  NULL AS RATIO_FROM,
  NULL AS STORE_CTRY_ABBR,
  NULL AS CURRENCY_NBR
FROM
  FIL_TRUNC_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_currency_day_TRUNC")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_currency_day_TRUNC", mainWorkflowId, parentName)
