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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_carrier_profile_pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, SQ_Shortcut_to_CARRIER_PROFILE_1


query_1 = f"""SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  SHIP_VIA AS WMS_SHIP_VIA,
  SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  CASE
    WHEN RANK() OVER (
      PARTITION BY
        NVL(SCAC_CD, SHIP_VIA)
      ORDER BY
        LENGTH(NVL(SCM_CARRIER_ID, ' ')),
        LENGTH(NVL(SCM_CARRIER_NAME, ' ')) DESC
    ) = 1 THEN 1
    ELSE 0
  END AS PRIMARY_CARRIER_IND,
  UPDATE_FLAG AS UPDATE_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      T1.CARRIER_ID,
      T2.SCAC_CD,
      T2.SCM_CARRIER_ID,
      T2.SCM_CARRIER_NAME,
      T1.SHIP_VIA,
      T1.SHIP_VIA_DESC,
      1 AS UPDATE_FLAG
    FROM
      (
        SELECT
          C.CARRIER_ID,
          W.SHIP_VIA,
          W.SHIP_VIA_DESC
        FROM
          CARRIER_PROFILE_WMS_PRE W,
          Shortcut_to_CARRIER_PROFILE_0 C
        WHERE
          W.SHIP_VIA = C.WMS_SHIP_VIA
      ) T1
      LEFT OUTER JOIN (
        SELECT
          C.CARRIER_ID,
          T.SCAC_CD,
          T.SCM_CARRIER_ID,
          T.SCM_CARRIER_NAME
        FROM
          scm_carrier T,
          Shortcut_to_CARRIER_PROFILE_0 C
        WHERE
          T.SCM_CARRIER_ID = C.SCM_CARRIER_ID
      ) T2 ON T1.CARRIER_ID = T2.CARRIER_ID
    UNION ALL
    SELECT
      T2.CARRIER_ID,
      T2.SCAC_CD,
      T2.SCM_CARRIER_ID,
      T2.SCM_CARRIER_NAME,
      T1.SHIP_VIA,
      T1.SHIP_VIA_DESC,
      1 AS UPDATE_FLAG
    FROM
      (
        SELECT
          C.CARRIER_ID,
          T.SCAC_CD,
          T.SCM_CARRIER_ID,
          T.SCM_CARRIER_NAME
        FROM
          SCM_CARRIER T,
          Shortcut_to_CARRIER_PROFILE_0 C
        WHERE
          T.SCM_CARRIER_ID = C.SCM_CARRIER_ID
      ) T2
      LEFT OUTER JOIN (
        SELECT
          C.CARRIER_ID,
          W.SHIP_VIA,
          W.SHIP_VIA_DESC
        FROM
          CARRIER_PROFILE_WMS_PRE W,
          Shortcut_to_CARRIER_PROFILE_0 C
        WHERE
          W.SHIP_VIA = C.WMS_SHIP_VIA
      ) T1 ON T2.CARRIER_ID = T1.CARRIER_ID
    WHERE
      T1.CARRIER_ID IS NULL
    UNION ALL
    SELECT
      NEXT VALUE FOR CARRIER_ID_SEQ AS CARRIER_ID,
      T.SCAC_CD,
      T.SCM_CARRIER_ID,
      T.SCM_CARRIER_NAME,
      T1.SHIP_VIA,
      T1.SHIP_VIA_DESC,
      0 AS UPDATE_FLAG
    FROM
      (
        SELECT
          W.SHIP_VIA,
          W.SHIP_VIA_DESC
        FROM
          CARRIER_PROFILE_WMS_PRE W
          LEFT OUTER JOIN Shortcut_to_CARRIER_PROFILE_0 C ON W.SHIP_VIA = C.WMS_SHIP_VIA
        WHERE
          C.CARRIER_ID IS NULL
      ) T1
      LEFT OUTER JOIN SCM_CARRIER T ON T1.SHIP_VIA = T.SCAC_CD
    UNION ALL
    SELECT
      NEXT VALUE FOR CARRIER_ID_SEQ AS CARRIER_ID,
      T1.SCAC_CD,
      T1.SCM_CARRIER_ID,
      T1.SCM_CARRIER_NAME,
      W.SHIP_VIA,
      W.SHIP_VIA_DESC,
      0 AS UPDATE_FLAG
    FROM
      (
        SELECT
          T.SCAC_CD,
          T.SCM_CARRIER_ID,
          T.SCM_CARRIER_NAME
        FROM
          SCM_CARRIER T
          LEFT OUTER JOIN Shortcut_to_CARRIER_PROFILE_0 C ON T.SCM_CARRIER_ID = C.SCM_CARRIER_ID
        WHERE
          C.CARRIER_ID IS NULL
      ) T1
      LEFT OUTER JOIN CARRIER_PROFILE_WMS_PRE W ON T1.SCAC_CD = W.SHIP_VIA
    WHERE
      W.SHIP_VIA IS NULL
  ) TEMP"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_CARRIER_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SCM_CARRIER_2


query_2 = f"""SELECT
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  SCAC_CD AS SCAC_CD,
  DEDICATED_FLAG AS DEDICATED_FLAG,
  ADDRESS AS ADDRESS,
  CITY AS CITY,
  STATE AS STATE,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTRY_CODE AS COUNTRY_CODE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SCM_CARRIER"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SCM_CARRIER_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CARRIER_PROFILE_WMS_PRE_3


query_3 = f"""SELECT
  SHIP_VIA AS SHIP_VIA,
  SHIP_VIA_DESC AS SHIP_VIA_DESC
FROM
  CARRIER_PROFILE_WMS_PRE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_CARRIER_PROFILE_WMS_PRE_3")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE_PRE


spark.sql("""INSERT INTO
  CARRIER_PROFILE_PRE
SELECT
  CARRIER_ID AS CARRIER_ID,
  SCAC_CD AS SCAC_CD,
  SCM_CARRIER_ID AS SCM_CARRIER_ID,
  SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
  WMS_SHIP_VIA AS WMS_SHIP_VIA,
  WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
  PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
  UPDATE_FLAG AS UPDATE_FLAG
FROM
  SQ_Shortcut_to_CARRIER_PROFILE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_carrier_profile_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_carrier_profile_pre", mainWorkflowId, parentName)
