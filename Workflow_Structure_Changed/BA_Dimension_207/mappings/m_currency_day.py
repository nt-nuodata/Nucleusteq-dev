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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_currency_day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_currency_day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CURRENCY_0


query_0 = f"""SELECT
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
  CURRENCY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CURRENCY_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_CURRENCY_1


query_1 = f"""SELECT
  DAYS.DAY_DT AS DAY_DT,
  CURRENCY_ID AS CURRENCY_ID,
  DATE_RATE_START AS DATE_RATE_START,
  CURRENCY_TYPE AS CURRENCY_TYPE,
  DATE_RATE_ENDED AS DATE_RATE_ENDED,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  RATIO_TO AS RATIO_TO,
  RATIO_FROM AS RATIO_FROM,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  CURRENCY_NBR AS CURRENCY_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CURRENCY_0 C,
  DAYS
WHERE
  DAYS.DAY_DT BETWEEN DATE_RATE_START AND DATE_RATE_ENDED
  AND DAYS.DAY_DT < CURRENT_TIMESTAMP + INTERVAL '14 days'
  AND CURRENCY_ID = 'CAD'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_CURRENCY_1")

# COMMAND ----------
# DBTITLE 1, CURRENCY_DAY


spark.sql("""INSERT INTO
  CURRENCY_DAY
SELECT
  DAY_DT AS DAY_DT,
  TABLE_NAME AS CURRENCY_ID,
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
  ASQ_Shortcut_to_CURRENCY_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_currency_day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_currency_day", mainWorkflowId, parentName)
