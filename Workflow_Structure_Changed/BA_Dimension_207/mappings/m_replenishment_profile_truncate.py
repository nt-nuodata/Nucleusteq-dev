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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_profile_truncate")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_replenishment_profile_truncate", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, ASQ_Shortcut_to_WEEKS_1


query_1 = f"""SELECT
  123 AS CAL_WK,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_WEEKS_1")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_PROFILE


spark.sql("""INSERT INTO
  REPLENISHMENT_PROFILE
SELECT
  NULL AS PRODUCT_ID,
  NULL AS LOCATION_ID,
  NULL AS PROCURE_TYPE_CD,
  CAL_WK AS ROUND_VALUE_QTY,
  NULL AS REPL_TYPE_CD,
  NULL AS SAFETY_QTY,
  NULL AS SERVICE_LVL_RT,
  NULL AS ABC_INDICATOR_CD,
  NULL AS MAX_STOCK_QTY,
  NULL AS REORDER_POINT_QTY,
  NULL AS PERIOD_CD,
  NULL AS PLAN_DELIV_DAYS,
  NULL AS PROFIT_CENTER_ID,
  NULL AS MULTIPLIER_RT,
  NULL AS FDC_INDICATOR_CD,
  NULL AS ROUND_PROFILE_CD,
  NULL AS MAX_TARGET_STOCK_QTY,
  NULL AS MIN_TARGET_STOCK_QTY,
  NULL AS TARGET_STOCK_QTY,
  NULL AS TARGET_COVERAGE_CNT,
  NULL AS PRESENT_QTY,
  NULL AS POG_CAPACITY_QTY,
  NULL AS POG_LAST_CHNG_DT,
  NULL AS POG_FACINGS_QTY,
  NULL AS PROMO_END_DT,
  NULL AS PROMO_EFF_DT,
  NULL AS PROMO_QTY,
  NULL AS LAST_PULL_DT,
  NULL AS NEXT_PULL_DT,
  NULL AS PURCH_GROUP_ID,
  NULL AS BASIC_VALUE_QTY,
  NULL AS LAST_FC_DT,
  NULL AS LOAD_DT
FROM
  ASQ_Shortcut_to_WEEKS_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_profile_truncate")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_replenishment_profile_truncate", mainWorkflowId, parentName)
