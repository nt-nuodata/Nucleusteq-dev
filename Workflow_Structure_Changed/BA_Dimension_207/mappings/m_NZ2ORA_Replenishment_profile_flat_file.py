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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_Replenishment_profile_flat_file")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_Replenishment_profile_flat_file", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_REPLENISHMENT_PROFILE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  PROCURE_TYPE_CD AS PROCURE_TYPE_CD,
  ROUND_VALUE_QTY AS ROUND_VALUE_QTY,
  REPL_TYPE_CD AS REPL_TYPE_CD,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  ABC_INDICATOR_CD AS ABC_INDICATOR_CD,
  MAX_STOCK_QTY AS MAX_STOCK_QTY,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PERIOD_CD AS PERIOD_CD,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  PROFIT_CENTER_ID AS PROFIT_CENTER_ID,
  MULTIPLIER_RT AS MULTIPLIER_RT,
  FDC_INDICATOR_CD AS FDC_INDICATOR_CD,
  ROUND_PROFILE_CD AS ROUND_PROFILE_CD,
  MAX_TARGET_STOCK_QTY AS MAX_TARGET_STOCK_QTY,
  MIN_TARGET_STOCK_QTY AS MIN_TARGET_STOCK_QTY,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  TARGET_COVERAGE_CNT AS TARGET_COVERAGE_CNT,
  PRESENT_QTY AS PRESENT_QTY,
  POG_CAPACITY_QTY AS POG_CAPACITY_QTY,
  POG_LAST_CHNG_DT AS POG_LAST_CHNG_DT,
  POG_FACINGS_QTY AS POG_FACINGS_QTY,
  PROMO_END_DT AS PROMO_END_DT,
  PROMO_EFF_DT AS PROMO_EFF_DT,
  PROMO_QTY AS PROMO_QTY,
  LAST_PULL_DT AS LAST_PULL_DT,
  NEXT_PULL_DT AS NEXT_PULL_DT,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  BASIC_VALUE_QTY AS BASIC_VALUE_QTY,
  LAST_FC_DT AS LAST_FC_DT,
  LOAD_DT AS LOAD_DT
FROM
  REPLENISHMENT_PROFILE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_REPLENISHMENT_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REPLENISHMENT_PROFILE_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  ROUND_VALUE_QTY AS ROUND_VALUE_QTY,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  ROUND_PROFILE_CD AS ROUND_PROFILE_CD,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  POG_CAPACITY_QTY AS POG_CAPACITY_QTY,
  POG_FACINGS_QTY AS POG_FACINGS_QTY,
  PROMO_QTY AS PROMO_QTY,
  BASIC_VALUE_QTY AS BASIC_VALUE_QTY,
  LAST_FC_DT AS LAST_FC_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_REPLENISHMENT_PROFILE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_REPLENISHMENT_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_PROFILE_FLAT_FILE


spark.sql("""INSERT INTO
  REPLENISHMENT_PROFILE_FLAT_FILE
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  ROUND_VALUE_QTY AS ROUND_VALUE_QTY,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  ROUND_PROFILE_CD AS ROUND_PROFILE_CD,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  POG_CAPACITY_QTY AS POG_CAPACITY_QTY,
  POG_FACINGS_QTY AS POG_FACINGS_QTY,
  PROMO_QTY AS PROMO_QTY,
  BASIC_VALUE_QTY AS BASIC_VALUE_QTY,
  LAST_FC_DT AS LAST_FC_DT,
  LOAD_DT AS LOAD_DT
FROM
  SQ_Shortcut_to_REPLENISHMENT_PROFILE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_Replenishment_profile_flat_file")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_Replenishment_profile_flat_file", mainWorkflowId, parentName)
