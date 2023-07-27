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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile_wms_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_carrier_profile_wms_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WM_SHIP_VIA_0


query_0 = f"""SELECT
  WHSE AS WHSE,
  SHIP_VIA AS SHIP_VIA,
  SHIP_VIA_DESC AS SHIP_VIA_DESC,
  CARR_ID AS CARR_ID,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  DEL_FLG AS DEL_FLG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  WM_SHIP_VIA"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_WM_SHIP_VIA_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WM_SHIP_VIA_1


query_1 = f"""SELECT
  SHIP_VIA AS SHIP_VIA,
  MAX(UPPER(TRIM(SHIP_VIA_DESC))) AS SHIP_VIA_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WM_SHIP_VIA_0
WHERE
  DEL_FLG = 0
GROUP BY
  SHIP_VIA"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_WM_SHIP_VIA_1")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE_WMS_PRE


spark.sql("""INSERT INTO
  CARRIER_PROFILE_WMS_PRE
SELECT
  SHIP_VIA AS SHIP_VIA,
  SHIP_VIA_DESC AS SHIP_VIA_DESC
FROM
  SQ_Shortcut_to_WM_SHIP_VIA_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile_wms_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_carrier_profile_wms_pre", mainWorkflowId, parentName)
