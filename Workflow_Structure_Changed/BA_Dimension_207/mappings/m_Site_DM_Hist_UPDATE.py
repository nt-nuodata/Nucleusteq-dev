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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_DM_Hist_UPDATE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Site_DM_Hist_UPDATE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_DM_HIST_0


query_0 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  SITE_DM_END_DT AS SITE_DM_END_DT,
  CURRENT_DIST_MGR_CD AS CURRENT_DIST_MGR_CD,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_DM_HIST"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SITE_DM_HIST_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_DM_HIST_1


query_1 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      LOCATION_ID,
      SITE_DM_EFF_DT,
      DIST_MGR_NAME,
      SITE_DM_END_DT,
      CURRENT_DIST_MGR_CD,
      UPDATE_TSTMP,
      LOAD_TSTMP,
      RANK() OVER (
        PARTITION BY
          LOCATION_ID
        ORDER BY
          LOAD_TSTMP DESC
      ) AS RANK
    FROM
      Shortcut_to_SITE_DM_HIST_0
    WHERE
      CURRENT_DIST_MGR_CD = 'Y'
  ) a
WHERE
  RANK = 2"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_DM_HIST_1")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_LOGIC_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  'N' AS o_CURRENT_DIST_MGR_CD,
  now() AS o_UPDATE_TSTMP,
  ADD_TO_DATE(now(), 'DD', -1) AS o_SITE_DM_END_DT,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_DM_HIST_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_UPDATE_LOGIC_2")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_UPDATE_ONLY_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  o_SITE_DM_END_DT AS o_SITE_DM_END_DT,
  o_CURRENT_DIST_MGR_CD AS o_CURRENT_DIST_MGR_CD,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_UPDATE_LOGIC_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPDTRANS_UPDATE_ONLY_3")

# COMMAND ----------
# DBTITLE 1, SITE_DM_HIST


spark.sql("""MERGE INTO SITE_DM_HIST AS TARGET
USING
  UPDTRANS_UPDATE_ONLY_3 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.SITE_DM_EFF_DT = SOURCE.SITE_DM_EFF_DT
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.SITE_DM_EFF_DT = SOURCE.SITE_DM_EFF_DT,
  TARGET.DIST_MGR_NAME = SOURCE.DIST_MGR_NAME,
  TARGET.SITE_DM_END_DT = SOURCE.o_SITE_DM_END_DT,
  TARGET.CURRENT_DIST_MGR_CD = SOURCE.o_CURRENT_DIST_MGR_CD,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_DM_Hist_UPDATE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Site_DM_Hist_UPDATE", mainWorkflowId, parentName)
