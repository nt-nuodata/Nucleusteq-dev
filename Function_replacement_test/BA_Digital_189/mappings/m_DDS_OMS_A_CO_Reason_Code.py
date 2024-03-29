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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_CO_Reason_Code")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_CO_Reason_Code", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_CO_REASON_CODE_PRE_0


query_0 = f"""SELECT
  REASON_CODE_ID AS REASON_CODE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  REASON_CODE AS REASON_CODE,
  DESCRIPTION AS DESCRIPTION,
  REASON_CODE_TYPE AS REASON_CODE_TYPE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  SYSTEM_DEFINED AS SYSTEM_DEFINED,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_CO_REASON_CODE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_A_CO_REASON_CODE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_CO_REASON_CODE_PRE_1


query_1 = f"""SELECT
  REASON_CODE_ID AS REASON_CODE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  REASON_CODE AS REASON_CODE,
  DESCRIPTION AS DESCRIPTION,
  REASON_CODE_TYPE AS REASON_CODE_TYPE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  SYSTEM_DEFINED AS SYSTEM_DEFINED,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_CO_REASON_CODE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_CO_REASON_CODE_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_CO_REASON_CODE_2


query_2 = f"""SELECT
  OMS_REASON_CODE_ID AS OMS_REASON_CODE_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_REASON_CD AS OMS_REASON_CD,
  OMS_REASON_DESC AS OMS_REASON_DESC,
  OMS_REASON_CODE_TYPE AS OMS_REASON_CODE_TYPE,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  SYSTEM_DEFINED_FLAG AS SYSTEM_DEFINED_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_CO_REASON_CODE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_A_CO_REASON_CODE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_CO_REASON_CODE_3


query_3 = f"""SELECT
  OMS_REASON_CODE_ID AS OMS_REASON_CODE_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_REASON_CD AS OMS_REASON_CD,
  OMS_REASON_DESC AS OMS_REASON_DESC,
  OMS_REASON_CODE_TYPE AS OMS_REASON_CODE_TYPE,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  SYSTEM_DEFINED_FLAG AS SYSTEM_DEFINED_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_CO_REASON_CODE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_CO_REASON_CODE_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_A_CO_Reason_Code_4


query_4 = f"""SELECT
  DETAIL.REASON_CODE_ID AS REASON_CODE_ID,
  DETAIL.TC_COMPANY_ID AS TC_COMPANY_ID,
  DETAIL.REASON_CODE AS REASON_CODE,
  DETAIL.DESCRIPTION AS DESCRIPTION,
  DETAIL.REASON_CODE_TYPE AS REASON_CODE_TYPE,
  DETAIL.MARK_FOR_DELETION AS MARK_FOR_DELETION,
  DETAIL.SYSTEM_DEFINED AS SYSTEM_DEFINED,
  MASTER.OMS_REASON_CODE_ID AS lkp_OMS_REASON_CODE_ID,
  MASTER.OMS_TC_COMPANY_ID AS lkp_OMS_TC_COMPANY_ID,
  MASTER.OMS_REASON_CD AS lkp_OMS_REASON_CD,
  MASTER.OMS_REASON_DESC AS lkp_OMS_REASON_DESC,
  MASTER.OMS_REASON_CODE_TYPE AS lkp_OMS_REASON_CODE_TYPE,
  MASTER.MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  MASTER.SYSTEM_DEFINED_FLAG AS lkp_SYSTEM_DEFINED_FLAG,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_A_CO_REASON_CODE_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_A_CO_REASON_CODE_PRE_1 DETAIL ON MASTER.OMS_REASON_CODE_ID = DETAIL.REASON_CODE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_A_CO_Reason_Code_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  REASON_CODE_ID AS REASON_CODE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  REASON_CODE AS REASON_CODE,
  DESCRIPTION AS DESCRIPTION,
  REASON_CODE_TYPE AS REASON_CODE_TYPE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  SYSTEM_DEFINED AS SYSTEM_DEFINED,
  lkp_OMS_REASON_CODE_ID AS lkp_OMS_REASON_CODE_ID,
  lkp_OMS_TC_COMPANY_ID AS lkp_OMS_TC_COMPANY_ID,
  lkp_OMS_REASON_CD AS lkp_OMS_REASON_CD,
  lkp_OMS_REASON_DESC AS lkp_OMS_REASON_DESC,
  lkp_OMS_REASON_CODE_TYPE AS lkp_OMS_REASON_CODE_TYPE,
  lkp_MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  lkp_SYSTEM_DEFINED_FLAG AS lkp_SYSTEM_DEFINED_FLAG,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_A_CO_Reason_Code_4
WHERE
  ISNULL(lkp_OMS_REASON_CODE_ID)
  OR (
    NOT ISNULL(lkp_OMS_REASON_CODE_ID)
    AND (
      IFF(
        ISNULL(LTRIM(RTRIM(REASON_CODE))),
        ' ',
        LTRIM(RTRIM(REASON_CODE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_REASON_CD))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_REASON_CD))
      )
      OR IFF(
        ISNULL(TC_COMPANY_ID),
        TO_INTEGER(999999999),
        TC_COMPANY_ID
      ) <> IFF(
        ISNULL(lkp_OMS_TC_COMPANY_ID),
        TO_INTEGER(999999999),
        lkp_OMS_TC_COMPANY_ID
      )
      OR IFF(
        ISNULL(LTRIM(RTRIM(DESCRIPTION))),
        ' ',
        LTRIM(RTRIM(DESCRIPTION))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_REASON_DESC))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_REASON_DESC))
      )
      OR IFF(
        ISNULL(REASON_CODE_TYPE),
        TO_INTEGER(999999999),
        REASON_CODE_TYPE
      ) <> IFF(
        ISNULL(lkp_OMS_REASON_CODE_TYPE),
        TO_INTEGER(999999999),
        lkp_OMS_REASON_CODE_TYPE
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
        ISNULL(SYSTEM_DEFINED),
        TO_INTEGER(999999999),
        SYSTEM_DEFINED
      ) <> IFF(
        ISNULL(lkp_SYSTEM_DEFINED_FLAG),
        TO_INTEGER(999999999),
        lkp_SYSTEM_DEFINED_FLAG
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FTR_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_FLAG_6


query_6 = f"""SELECT
  REASON_CODE_ID AS REASON_CODE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  REASON_CODE AS REASON_CODE,
  DESCRIPTION AS DESCRIPTION,
  REASON_CODE_TYPE AS REASON_CODE_TYPE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  SYSTEM_DEFINED AS SYSTEM_DEFINED,
  lkp_OMS_REASON_CODE_ID AS lkp_OMS_REASON_CODE_ID,
  lkp_OMS_TC_COMPANY_ID AS lkp_OMS_TC_COMPANY_ID,
  lkp_OMS_REASON_CD AS lkp_OMS_REASON_CD,
  lkp_OMS_REASON_DESC AS lkp_OMS_REASON_DESC,
  lkp_OMS_REASON_CODE_TYPE AS lkp_OMS_REASON_CODE_TYPE,
  lkp_MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  lkp_SYSTEM_DEFINED_FLAG AS lkp_SYSTEM_DEFINED_FLAG,
  now() AS UPDATE_TSTMP_exp,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_REASON_CODE_ID), 1, 2) AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  REASON_CODE_ID AS REASON_CODE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  REASON_CODE AS REASON_CODE,
  DESCRIPTION AS DESCRIPTION,
  REASON_CODE_TYPE AS REASON_CODE_TYPE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  SYSTEM_DEFINED AS SYSTEM_DEFINED,
  UPDATE_TSTMP_exp AS UPDATE_TSTMP_exp,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  UPDATE_FLAG AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(UPDATE_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_UPDATE_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_A_CO_REASON_CODE


spark.sql("""MERGE INTO OMS_A_CO_REASON_CODE AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_REASON_CODE_ID = SOURCE.REASON_CODE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_REASON_CODE_ID = SOURCE.REASON_CODE_ID,
  TARGET.OMS_TC_COMPANY_ID = SOURCE.TC_COMPANY_ID,
  TARGET.OMS_REASON_CD = SOURCE.REASON_CODE,
  TARGET.OMS_REASON_DESC = SOURCE.DESCRIPTION,
  TARGET.OMS_REASON_CODE_TYPE = SOURCE.REASON_CODE_TYPE,
  TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION,
  TARGET.SYSTEM_DEFINED_FLAG = SOURCE.SYSTEM_DEFINED,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_TC_COMPANY_ID = SOURCE.TC_COMPANY_ID
  AND TARGET.OMS_REASON_CD = SOURCE.REASON_CODE
  AND TARGET.OMS_REASON_DESC = SOURCE.DESCRIPTION
  AND TARGET.OMS_REASON_CODE_TYPE = SOURCE.REASON_CODE_TYPE
  AND TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION
  AND TARGET.SYSTEM_DEFINED_FLAG = SOURCE.SYSTEM_DEFINED
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_REASON_CODE_ID,
    TARGET.OMS_TC_COMPANY_ID,
    TARGET.OMS_REASON_CD,
    TARGET.OMS_REASON_DESC,
    TARGET.OMS_REASON_CODE_TYPE,
    TARGET.MARK_FOR_DELETION_FLAG,
    TARGET.SYSTEM_DEFINED_FLAG,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.REASON_CODE_ID,
    SOURCE.TC_COMPANY_ID,
    SOURCE.REASON_CODE,
    SOURCE.DESCRIPTION,
    SOURCE.REASON_CODE_TYPE,
    SOURCE.MARK_FOR_DELETION,
    SOURCE.SYSTEM_DEFINED,
    SOURCE.UPDATE_TSTMP_exp,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_CO_Reason_Code")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_CO_Reason_Code", mainWorkflowId, parentName)
