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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pog_ztb_promohst_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pog_ztb_promohst_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_POG_ZTB_PROMO_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  POG_NBR AS POG_NBR,
  REPL_START_DT AS REPL_START_DT,
  REPL_END_DT AS REPL_END_DT,
  LIST_START_DT AS LIST_START_DT,
  LIST_END_DT AS LIST_END_DT,
  PROMO_QTY AS PROMO_QTY,
  LAST_CHNG_DT AS LAST_CHNG_DT,
  POG_STATUS AS POG_STATUS,
  DELETE_IND AS DELETE_IND,
  DATE_ADDED AS DATE_ADDED,
  DATE_REFRESHED AS DATE_REFRESHED,
  DATE_DELETED AS DATE_DELETED
FROM
  POG_ZTB_PROMO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_POG_ZTB_PROMO_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_POG_ZTB_PROMO_1


query_1 = f"""SELECT
  Shortcut_to_POG_ZTB_PROMO_0.SKU_NBR AS SKU_NBR,
  Shortcut_to_POG_ZTB_PROMO_0.STORE_NBR AS STORE_NBR,
  Shortcut_to_POG_ZTB_PROMO_0.POG_NBR AS POG_NBR,
  Shortcut_to_POG_ZTB_PROMO_0.REPL_START_DT AS REPL_START_DT,
  Shortcut_to_POG_ZTB_PROMO_0.REPL_END_DT AS REPL_END_DT,
  Shortcut_to_POG_ZTB_PROMO_0.LIST_START_DT AS LIST_START_DT,
  Shortcut_to_POG_ZTB_PROMO_0.LIST_END_DT AS LIST_END_DT,
  Shortcut_to_POG_ZTB_PROMO_0.PROMO_QTY AS PROMO_QTY,
  Shortcut_to_POG_ZTB_PROMO_0.LAST_CHNG_DT AS LAST_CHNG_DT,
  Shortcut_to_POG_ZTB_PROMO_0.POG_STATUS AS POG_STATUS,
  Shortcut_to_POG_ZTB_PROMO_0.DELETE_IND AS DELETE_IND,
  Shortcut_to_POG_ZTB_PROMO_0.DATE_ADDED AS DATE_ADDED,
  Shortcut_to_POG_ZTB_PROMO_0.DATE_REFRESHED AS DATE_REFRESHED,
  Shortcut_to_POG_ZTB_PROMO_0.DATE_DELETED AS DATE_DELETED,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_POG_ZTB_PROMO_0
WHERE
  Shortcut_to_POG_ZTB_PROMO_0.DELETE_IND = ' '"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_POG_ZTB_PROMO_1")

# COMMAND ----------
# DBTITLE 1, POG_ZTB_PROMOHST_PRE


spark.sql("""INSERT INTO
  POG_ZTB_PROMOHST_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  POG_NBR AS POG_NBR,
  REPL_START_DT AS REPL_START_DT,
  REPL_END_DT AS REPL_END_DT,
  LIST_START_DT AS LIST_START_DT,
  LIST_END_DT AS LIST_END_DT,
  PROMO_QTY AS PROMO_QTY,
  LAST_CHNG_DT AS LAST_CHNG_DT,
  POG_STATUS AS POG_STATUS,
  DELETE_IND AS DELETE_IND,
  DATE_ADDED AS DATE_ADDED,
  DATE_REFRESHED AS DATE_REFRESHED,
  DATE_DELETED AS DATE_DELETED
FROM
  ASQ_Shortcut_to_POG_ZTB_PROMO_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pog_ztb_promohst_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pog_ztb_promohst_pre", mainWorkflowId, parentName)
