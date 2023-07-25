# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Role")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Role", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ROLE_PRE_0


query_0 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ROLE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ROLE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ROLE_PRE_1


query_1 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ROLE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ROLE_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ROLE_2


query_2 = f"""SELECT
  OMS_ROLE_ID AS OMS_ROLE_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  OMS_ROLE_NAME AS OMS_ROLE_NAME,
  OMS_ROLE_DESC AS OMS_ROLE_DESC,
  OMS_ROLE_TYPE_ID AS OMS_ROLE_TYPE_ID,
  ACTIVE_FLAG AS ACTIVE_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ROLE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_ROLE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ROLE_3


query_3 = f"""SELECT
  OMS_ROLE_ID AS OMS_ROLE_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  OMS_ROLE_NAME AS OMS_ROLE_NAME,
  OMS_ROLE_DESC AS OMS_ROLE_DESC,
  OMS_ROLE_TYPE_ID AS OMS_ROLE_TYPE_ID,
  ACTIVE_FLAG AS ACTIVE_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ROLE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_ROLE_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_ROLE_4


query_4 = f"""SELECT
  DETAIL.ROLE_ID AS ROLE_ID,
  DETAIL.COMPANY_ID AS COMPANY_ID,
  DETAIL.ROLE_NAME AS ROLE_NAME,
  DETAIL.IS_ACTIVE AS IS_ACTIVE,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DETAIL.DESCRIPTION AS DESCRIPTION,
  MASTER.OMS_ROLE_ID AS lkp_OMS_ROLE_ID,
  MASTER.OMS_COMPANY_ID AS lkp_OMS_COMPANY_ID,
  MASTER.OMS_ROLE_NAME AS lkp_OMS_ROLE_NAME,
  MASTER.OMS_ROLE_DESC AS lkp_OMS_ROLE_DESC,
  MASTER.OMS_ROLE_TYPE_ID AS lkp_OMS_ROLE_TYPE_ID,
  MASTER.ACTIVE_FLAG AS lkp_ACTIVE_FLAG,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ROLE_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_ROLE_PRE_1 DETAIL ON MASTER.OMS_ROLE_ID = DETAIL.ROLE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_ROLE_4")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  lkp_OMS_ROLE_ID AS lkp_OMS_ROLE_ID,
  lkp_OMS_COMPANY_ID AS lkp_OMS_COMPANY_ID,
  lkp_OMS_ROLE_NAME AS lkp_OMS_ROLE_NAME,
  lkp_OMS_ROLE_DESC AS lkp_OMS_ROLE_DESC,
  lkp_OMS_ROLE_TYPE_ID AS lkp_OMS_ROLE_TYPE_ID,
  lkp_ACTIVE_FLAG AS lkp_ACTIVE_FLAG,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_ROLE_4
WHERE
  ISNULL(lkp_OMS_ROLE_ID)
  OR (
    NOT ISNULL(lkp_OMS_ROLE_ID)
    AND (
      IFF(
        ISNULL(COMPANY_ID),
        TO_INTEGER(999999999),
        COMPANY_ID
      ) <> IFF(
        ISNULL(lkp_OMS_COMPANY_ID),
        TO_INTEGER(999999999),
        lkp_OMS_COMPANY_ID
      )
      OR IFF(
        ISNULL(LTRIM(RTRIM(ROLE_NAME))),
        ' ',
        LTRIM(RTRIM(ROLE_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_ROLE_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_ROLE_NAME))
      )
      OR IFF(
        ISNULL(LTRIM(RTRIM(DESCRIPTION))),
        ' ',
        LTRIM(RTRIM(DESCRIPTION))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_ROLE_DESC))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_ROLE_DESC))
      )
      OR IFF(
        ISNULL(ROLE_TYPE_ID),
        TO_INTEGER(999999999),
        ROLE_TYPE_ID
      ) <> IFF(
        ISNULL(lkp_OMS_ROLE_TYPE_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ROLE_TYPE_ID
      )
      OR IFF(ISNULL(IS_ACTIVE), TO_INTEGER(9999), IS_ACTIVE) <> IFF(
        ISNULL(lkp_ACTIVE_FLAG),
        TO_INTEGER(9999),
        lkp_ACTIVE_FLAG
      )
      OR IFF(
        ISNULL(CREATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        CREATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_CREATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_CREATED_TSTMP
      )
      OR IFF(
        ISNULL(LAST_UPDATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        LAST_UPDATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_LAST_UPDATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_LAST_UPDATED_TSTMP
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_FLAG_6


query_6 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  now() AS o_UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS o_LOAD_TSTMP,
  IFF(ISNULL(lkp_OMS_ROLE_ID), 1, 2) AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_UPDATE_7


query_7 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  o_UPDATE_FLAG AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_UPDATE_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_UPDATE_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INSERT_UPDATE_7")

# COMMAND ----------
# DBTITLE 1, OMS_ROLE


spark.sql("""MERGE INTO OMS_ROLE AS TARGET
USING
  UPD_INSERT_UPDATE_7 AS SOURCE ON TARGET.OMS_ROLE_ID = SOURCE.ROLE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_ROLE_ID = SOURCE.ROLE_ID,
  TARGET.OMS_COMPANY_ID = SOURCE.COMPANY_ID,
  TARGET.OMS_ROLE_NAME = SOURCE.ROLE_NAME,
  TARGET.OMS_ROLE_DESC = SOURCE.DESCRIPTION,
  TARGET.OMS_ROLE_TYPE_ID = SOURCE.ROLE_TYPE_ID,
  TARGET.ACTIVE_FLAG = SOURCE.IS_ACTIVE,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_COMPANY_ID = SOURCE.COMPANY_ID
  AND TARGET.OMS_ROLE_NAME = SOURCE.ROLE_NAME
  AND TARGET.OMS_ROLE_DESC = SOURCE.DESCRIPTION
  AND TARGET.OMS_ROLE_TYPE_ID = SOURCE.ROLE_TYPE_ID
  AND TARGET.ACTIVE_FLAG = SOURCE.IS_ACTIVE
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_ROLE_ID,
    TARGET.OMS_COMPANY_ID,
    TARGET.OMS_ROLE_NAME,
    TARGET.OMS_ROLE_DESC,
    TARGET.OMS_ROLE_TYPE_ID,
    TARGET.ACTIVE_FLAG,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ROLE_ID,
    SOURCE.COMPANY_ID,
    SOURCE.ROLE_NAME,
    SOURCE.DESCRIPTION,
    SOURCE.ROLE_TYPE_ID,
    SOURCE.IS_ACTIVE,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Role")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Role", mainWorkflowId, parentName)
