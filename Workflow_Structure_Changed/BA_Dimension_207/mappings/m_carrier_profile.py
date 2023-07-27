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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_carrier_profile", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CARRIER_PROFILE_0


query_0 = f"""SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  WMS_SHIP_VIA AS WMS_SHIP_VIA,
  WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  CARRIER_PROFILE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CARRIER_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CARRIER_PROFILE_PRE_1


query_1 = f"""SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  WMS_SHIP_VIA AS WMS_SHIP_VIA,
  WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  UPDATE_FLAG AS UPDATE_FLAG
FROM
  CARRIER_PROFILE_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_CARRIER_PROFILE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CARRIER_PROFILE_PRE_2


query_2 = f"""SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  WMS_SHIP_VIA AS WMS_SHIP_VIA,
  WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  CURRENT_DATE AS UPDATE_DT,
  CURRENT_DATE AS LOAD_DT,
  UPDATE_FLAG AS UPDATE_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CARRIER_PROFILE_PRE_1
WHERE
  UPDATE_FLAG = 0
UNION ALL
SELECT
  P.CARRIER_ID AS CARRIER_ID,
  P.SCAC_CD AS SCAC_CD,
  P.SCM_CARRIER_ID AS SCM_CARRIER_ID,
  P.SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  P.WMS_SHIP_VIA AS WMS_SHIP_VIA,
  P.WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  P.PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  CURRENT_DATE AS UPDATE_DT,
  NVL(C.LOAD_DT, CURRENT_DATE) AS LOAD_DT,
  UPDATE_FLAG AS UPDATE_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CARRIER_PROFILE_PRE_1 P,
  Shortcut_to_CARRIER_PROFILE_0 C
WHERE
  UPDATE_FLAG = 1
  AND P.CARRIER_ID = C.CARRIER_ID
  AND (
    NVL(P.SCAC_CD, ' ') <> NVL(C.SCAC_CD, ' ')
    OR NVL(P.SCM_CARRIER_ID, ' ') <> NVL(C.SCM_CARRIER_ID, ' ')
    OR NVL(P.SCM_CARRIER_NAME, ' ') <> NVL(C.SCM_CARRIER_NAME, ' ')
    OR NVL(P.WMS_SHIP_VIA, ' ') <> NVL(C.WMS_SHIP_VIA, ' ')
    OR NVL(P.WMS_SHIP_VIA_DESC, ' ') <> NVL(C.WMS_SHIP_VIA_DESC, ' ')
    OR P.PRIMARY_CARRIER_IND <> C.PRIMARY_CARRIER_IND
  )"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_CARRIER_PROFILE_PRE_2")

# COMMAND ----------
# DBTITLE 1, UPD_ins_upd_3


query_3 = f"""SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  WMS_SHIP_VIA AS WMS_SHIP_VIA,
  WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  UPDATE_FLAG AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(UPDATE_FLAG = 0, 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  SQ_Shortcut_to_CARRIER_PROFILE_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_ins_upd_3")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE


spark.sql("""MERGE INTO CARRIER_PROFILE AS TARGET
USING
  UPD_ins_upd_3 AS SOURCE ON TARGET.CARRIER_ID = SOURCE.CARRIER_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.CARRIER_ID = SOURCE.CARRIER_ID,
  TARGET.SCAC_CD = SOURCE.SCAC_CD,
  TARGET.SCM_CARRIER_ID = SOURCE.SCM_CARRIER_ID,
  TARGET.SCM_CARRIER_NAME = SOURCE.SCM_CARRIER_NAME,
  TARGET.WMS_SHIP_VIA = SOURCE.WMS_SHIP_VIA,
  TARGET.WMS_SHIP_VIA_DESC = SOURCE.WMS_SHIP_VIA_DESC,
  TARGET.PRIMARY_CARRIER_IND = SOURCE.PRIMARY_CARRIER_IND,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SCAC_CD = SOURCE.SCAC_CD
  AND TARGET.SCM_CARRIER_ID = SOURCE.SCM_CARRIER_ID
  AND TARGET.SCM_CARRIER_NAME = SOURCE.SCM_CARRIER_NAME
  AND TARGET.WMS_SHIP_VIA = SOURCE.WMS_SHIP_VIA
  AND TARGET.WMS_SHIP_VIA_DESC = SOURCE.WMS_SHIP_VIA_DESC
  AND TARGET.PRIMARY_CARRIER_IND = SOURCE.PRIMARY_CARRIER_IND
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.CARRIER_ID,
    TARGET.SCAC_CD,
    TARGET.SCM_CARRIER_ID,
    TARGET.SCM_CARRIER_NAME,
    TARGET.WMS_SHIP_VIA,
    TARGET.WMS_SHIP_VIA_DESC,
    TARGET.PRIMARY_CARRIER_IND,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.CARRIER_ID,
    SOURCE.SCAC_CD,
    SOURCE.SCM_CARRIER_ID,
    SOURCE.SCM_CARRIER_NAME,
    SOURCE.WMS_SHIP_VIA,
    SOURCE.WMS_SHIP_VIA_DESC,
    SOURCE.PRIMARY_CARRIER_IND,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_carrier_profile", mainWorkflowId, parentName)
