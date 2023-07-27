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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_store_data")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_store_data", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STORE_DATA_0


query_0 = f"""SELECT
  SITE_NBR AS SITE_NBR,
  COMPANY_CD AS COMPANY_CD,
  CURRENCY_CD AS CURRENCY_CD,
  MERCH_ID AS MERCH_ID,
  TAX_JURISDICTION_CD AS TAX_JURISDICTION_CD,
  EARLIEST_VALUE_POST_DT AS EARLIEST_VALUE_POST_DT,
  TOT_VALUE_WARNING_LMT AS TOT_VALUE_WARNING_LMT,
  TOT_VALUE_ERROR_LMT AS TOT_VALUE_ERROR_LMT,
  TRANS_CNT_LOWER_LMT AS TRANS_CNT_LOWER_LMT,
  TRANS_CNT_WARNING_LMT AS TRANS_CNT_WARNING_LMT,
  TRANS_CNT_ERR_LMT AS TRANS_CNT_ERR_LMT,
  TRANS_VALUE_ERR_LMT AS TRANS_VALUE_ERR_LMT,
  TRANS_SEQ_GAP_CNT_ERR_LMT AS TRANS_SEQ_GAP_CNT_ERR_LMT,
  OUT_OF_BAL_CNT AS OUT_OF_BAL_CNT,
  OUT_OF_BAL_VALUE AS OUT_OF_BAL_VALUE,
  BANK_DEPOSIT_VAR_PCT AS BANK_DEPOSIT_VAR_PCT,
  CREATE_DT AS CREATE_DT,
  CREATE_USER AS CREATE_USER,
  UPDATE_DT AS UPDATE_DT,
  UPDATE_USER AS UPDATE_USER
FROM
  STORE_DATA"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STORE_DATA_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_STORE_DATA_1


query_1 = f"""SELECT
  SITE_NBR AS SITE_NBR,
  COMPANY_CD AS COMPANY_CD,
  CURRENCY_CD AS CURRENCY_CD,
  MERCH_ID AS MERCH_ID,
  TAX_JURISDICTION_CD AS TAX_JURISDICTION_CD,
  EARLIEST_VALUE_POST_DT AS EARLIEST_VALUE_POST_DT,
  TOT_VALUE_WARNING_LMT AS TOT_VALUE_WARNING_LMT,
  TOT_VALUE_ERROR_LMT AS TOT_VALUE_ERROR_LMT,
  TRANS_CNT_LOWER_LMT AS TRANS_CNT_LOWER_LMT,
  TRANS_CNT_WARNING_LMT AS TRANS_CNT_WARNING_LMT,
  TRANS_CNT_ERR_LMT AS TRANS_CNT_ERR_LMT,
  TRANS_VALUE_ERR_LMT AS TRANS_VALUE_ERR_LMT,
  TRANS_SEQ_GAP_CNT_ERR_LMT AS TRANS_SEQ_GAP_CNT_ERR_LMT,
  OUT_OF_BAL_CNT AS OUT_OF_BAL_CNT,
  OUT_OF_BAL_VALUE AS OUT_OF_BAL_VALUE,
  BANK_DEPOSIT_VAR_PCT AS BANK_DEPOSIT_VAR_PCT,
  CREATE_DT AS CREATE_DT,
  CREATE_USER AS CREATE_USER,
  UPDATE_DT AS UPDATE_DT,
  UPDATE_USER AS UPDATE_USER,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_STORE_DATA_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_STORE_DATA_1")

# COMMAND ----------
# DBTITLE 1, STORE_DATA


spark.sql("""INSERT INTO
  STORE_DATA
SELECT
  SITE_NBR AS SITE_NBR,
  COMPANY_CD AS COMPANY_CD,
  CURRENCY_CD AS CURRENCY_CD,
  MERCH_ID AS MERCH_ID,
  TAX_JURISDICTION_CD AS TAX_JURISDICTION_CD,
  EARLIEST_VALUE_POST_DT AS EARLIEST_VALUE_POST_DT,
  TOT_VALUE_WARNING_LMT AS TOT_VALUE_WARNING_LMT,
  TOT_VALUE_ERROR_LMT AS TOT_VALUE_ERROR_LMT,
  TRANS_CNT_LOWER_LMT AS TRANS_CNT_LOWER_LMT,
  TRANS_CNT_WARNING_LMT AS TRANS_CNT_WARNING_LMT,
  TRANS_CNT_ERR_LMT AS TRANS_CNT_ERR_LMT,
  TRANS_VALUE_ERR_LMT AS TRANS_VALUE_ERR_LMT,
  TRANS_SEQ_GAP_CNT_ERR_LMT AS TRANS_SEQ_GAP_CNT_ERR_LMT,
  OUT_OF_BAL_CNT AS OUT_OF_BAL_CNT,
  OUT_OF_BAL_VALUE AS OUT_OF_BAL_VALUE,
  BANK_DEPOSIT_VAR_PCT AS BANK_DEPOSIT_VAR_PCT,
  CREATE_DT AS CREATE_DT,
  CREATE_USER AS CREATE_USER,
  UPDATE_DT AS UPDATE_DT,
  UPDATE_USER AS UPDATE_USER
FROM
  SQ_Shortcut_to_STORE_DATA_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_store_data")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_store_data", mainWorkflowId, parentName)
