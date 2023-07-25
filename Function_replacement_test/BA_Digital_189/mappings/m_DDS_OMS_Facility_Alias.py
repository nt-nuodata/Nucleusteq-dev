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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Alias")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Facility_Alias", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_FACILITY_ALIAS_PRE_0


query_0 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_FACILITY_ALIAS_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_FACILITY_ALIAS_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_FACILITY_ALIAS_PRE_1


query_1 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_FACILITY_ALIAS_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_FACILITY_ALIAS_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_FACILITY_ALIAS_2


query_2 = f"""SELECT
  OMS_FACILITY_ALIAS_ID AS OMS_FACILITY_ALIAS_ID,
  OMS_FACILITY_ID AS OMS_FACILITY_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_FACILITY_NAME AS OMS_FACILITY_NAME,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_FACILITY_ALIAS"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_FACILITY_ALIAS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_FACILITY_ALIAS_3


query_3 = f"""SELECT
  OMS_FACILITY_ALIAS_ID AS OMS_FACILITY_ALIAS_ID,
  OMS_FACILITY_ID AS OMS_FACILITY_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_FACILITY_NAME AS OMS_FACILITY_NAME,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_FACILITY_ALIAS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_FACILITY_ALIAS_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_FACILITY_ALIAS_4


query_4 = f"""SELECT
  DETAIL.FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  DETAIL.FACILITY_ID AS FACILITY_ID,
  DETAIL.TC_COMPANY_ID AS TC_COMPANY_ID,
  DETAIL.FACILITY_NAME AS FACILITY_NAME,
  DETAIL.MARK_FOR_DELETION AS MARK_FOR_DELETION,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MASTER.OMS_FACILITY_ALIAS_ID AS lkp_OMS_FACILITY_ALIAS_ID,
  MASTER.OMS_FACILITY_ID AS lkp_OMS_FACILITY_ID,
  MASTER.OMS_TC_COMPANY_ID AS lkp_OMS_TC_COMPANY_ID,
  MASTER.OMS_FACILITY_NAME AS lkp_OMS_FACILITY_NAME,
  MASTER.MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_FACILITY_ALIAS_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_FACILITY_ALIAS_PRE_1 DETAIL ON MASTER.OMS_FACILITY_ALIAS_ID = DETAIL.FACILITY_ALIAS_ID
  AND MASTER.OMS_FACILITY_ID = DETAIL.FACILITY_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_FACILITY_ALIAS_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_REC_5


query_5 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  lkp_OMS_FACILITY_ALIAS_ID AS lkp_OMS_FACILITY_ALIAS_ID,
  lkp_OMS_FACILITY_ID AS lkp_OMS_FACILITY_ID,
  lkp_OMS_TC_COMPANY_ID AS lkp_OMS_TC_COMPANY_ID,
  lkp_OMS_FACILITY_NAME AS lkp_OMS_FACILITY_NAME,
  lkp_MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_FACILITY_ALIAS_4
WHERE
  ISNULL(lkp_OMS_FACILITY_ALIAS_ID)
  OR (
    NOT ISNULL(lkp_OMS_FACILITY_ALIAS_ID)
    AND (
      IFF(
        ISNULL(TC_COMPANY_ID),
        TO_INTEGER(999999999),
        TC_COMPANY_ID
      ) <> IFF(
        ISNULL(lkp_OMS_TC_COMPANY_ID),
        TO_INTEGER(999999999),
        lkp_OMS_TC_COMPANY_ID
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(FACILITY_NAME))),
        ' ',
        LTRIM(RTRIM(FACILITY_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_FACILITY_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_FACILITY_NAME))
      )
      OR IFF(
        ISNULL(MARK_FOR_DELETION),
        TO_INTEGER(999999999),
        MARK_FOR_DELETION
      ) <> IFF(
        ISNULL(lkp_MARK_FOR_DELETION_FLAG),
        TO_INTEGER(999999999),
        lkp_MARK_FOR_DELETION_FLAG
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

df_5.createOrReplaceTempView("FTR_UNCHANGED_REC_5")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAG_6


query_6 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  lkp_OMS_FACILITY_ALIAS_ID AS lkp_OMS_FACILITY_ALIAS_ID,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_FACILITY_ALIAS_ID), 1, 2) AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_REC_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_VALID_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  o_VALID_UPDATOR AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_VALID_UPDATOR, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_FACILITY_ALIAS


spark.sql("""MERGE INTO OMS_FACILITY_ALIAS AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_FACILITY_ALIAS_ID = SOURCE.FACILITY_ALIAS_ID
  AND TARGET.OMS_FACILITY_ID = SOURCE.FACILITY_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_FACILITY_ALIAS_ID = SOURCE.FACILITY_ALIAS_ID,
  TARGET.OMS_FACILITY_ID = SOURCE.FACILITY_ID,
  TARGET.OMS_TC_COMPANY_ID = SOURCE.TC_COMPANY_ID,
  TARGET.OMS_FACILITY_NAME = SOURCE.FACILITY_NAME,
  TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_TC_COMPANY_ID = SOURCE.TC_COMPANY_ID
  AND TARGET.OMS_FACILITY_NAME = SOURCE.FACILITY_NAME
  AND TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_FACILITY_ALIAS_ID,
    TARGET.OMS_FACILITY_ID,
    TARGET.OMS_TC_COMPANY_ID,
    TARGET.OMS_FACILITY_NAME,
    TARGET.MARK_FOR_DELETION_FLAG,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.FACILITY_ALIAS_ID,
    SOURCE.FACILITY_ID,
    SOURCE.TC_COMPANY_ID,
    SOURCE.FACILITY_NAME,
    SOURCE.MARK_FOR_DELETION,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Alias")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Facility_Alias", mainWorkflowId, parentName)