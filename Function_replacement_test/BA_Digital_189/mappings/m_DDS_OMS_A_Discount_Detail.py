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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Detail")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Discount_Detail", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_0


query_0 = f"""SELECT
  DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_ID AS DISCOUNT_ID,
  EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  REASON_ID AS REASON_ID,
  DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_DISCOUNT_DETAIL_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_1


query_1 = f"""SELECT
  DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_ID AS DISCOUNT_ID,
  EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  REASON_ID AS REASON_ID,
  DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_DISCOUNT_DETAIL_2


query_2 = f"""SELECT
  OMS_DISCOUNT_DETAIL_ID AS OMS_DISCOUNT_DETAIL_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  OMS_ENTITY_TYPE_ID AS OMS_ENTITY_TYPE_ID,
  OMS_ENTITY_ID AS OMS_ENTITY_ID,
  OMS_ENTITY_LINE_ID AS OMS_ENTITY_LINE_ID,
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_ID AS OMS_DISCOUNT_ID,
  OMS_EXT_DISCOUNT_ID AS OMS_EXT_DISCOUNT_ID,
  OMS_DISCOUNT_STATUS_ID AS OMS_DISCOUNT_STATUS_ID,
  OMS_REASON_ID AS OMS_REASON_ID,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_SOURCE_TYPE AS OMS_LAST_UPDATED_SOURCE_TYPE,
  OMS_LAST_UPDATED_SOURCE AS OMS_LAST_UPDATED_SOURCE,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_DISCOUNT_DETAIL"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_A_DISCOUNT_DETAIL_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_3


query_3 = f"""SELECT
  OMS_DISCOUNT_DETAIL_ID AS OMS_DISCOUNT_DETAIL_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  OMS_ENTITY_TYPE_ID AS OMS_ENTITY_TYPE_ID,
  OMS_ENTITY_ID AS OMS_ENTITY_ID,
  OMS_ENTITY_LINE_ID AS OMS_ENTITY_LINE_ID,
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_ID AS OMS_DISCOUNT_ID,
  OMS_EXT_DISCOUNT_ID AS OMS_EXT_DISCOUNT_ID,
  OMS_DISCOUNT_STATUS_ID AS OMS_DISCOUNT_STATUS_ID,
  OMS_REASON_ID AS OMS_REASON_ID,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  MARK_FOR_DELETION_FLAG AS MARK_FOR_DELETION_FLAG,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_SOURCE_TYPE AS OMS_LAST_UPDATED_SOURCE_TYPE,
  OMS_LAST_UPDATED_SOURCE AS OMS_LAST_UPDATED_SOURCE,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_DISCOUNT_DETAIL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_A_DISCOUNT_DETAIL_4


query_4 = f"""SELECT
  DETAIL.DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  DETAIL.COMPANY_ID AS COMPANY_ID,
  DETAIL.ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  DETAIL.ENTITY_ID AS ENTITY_ID,
  DETAIL.ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DETAIL.DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DETAIL.DISCOUNT_ID AS DISCOUNT_ID,
  DETAIL.EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DETAIL.DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  DETAIL.LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.MARK_FOR_DELETION AS MARK_FOR_DELETION,
  DETAIL.REASON_ID AS REASON_ID,
  DETAIL.DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DETAIL.DISCOUNT_VALUE AS DISCOUNT_VALUE,
  MASTER.OMS_DISCOUNT_DETAIL_ID AS lkp_OMS_DISCOUNT_DETAIL_ID,
  MASTER.OMS_COMPANY_ID AS lkp_OMS_COMPANY_ID,
  MASTER.OMS_ENTITY_TYPE_ID AS lkp_OMS_ENTITY_TYPE_ID,
  MASTER.OMS_ENTITY_ID AS lkp_OMS_ENTITY_ID,
  MASTER.OMS_ENTITY_LINE_ID AS lkp_OMS_ENTITY_LINE_ID,
  MASTER.OMS_DISCOUNT_TYPE_ID AS lkp_OMS_DISCOUNT_TYPE_ID,
  MASTER.OMS_DISCOUNT_ID AS lkp_OMS_DISCOUNT_ID,
  MASTER.OMS_EXT_DISCOUNT_ID AS lkp_OMS_EXT_DISCOUNT_ID,
  MASTER.OMS_DISCOUNT_STATUS_ID AS lkp_OMS_DISCOUNT_STATUS_ID,
  MASTER.OMS_REASON_ID AS lkp_OMS_REASON_ID,
  MASTER.DISCOUNT_AMT AS lkp_DISCOUNT_AMT,
  MASTER.DISCOUNT_VALUE AS lkp_DISCOUNT_VALUE1,
  MASTER.MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_SOURCE_TYPE AS lkp_OMS_LAST_UPDATED_SOURCE_TYPE,
  MASTER.OMS_LAST_UPDATED_SOURCE AS lkp_OMS_LAST_UPDATED_SOURCE,
  MASTER.OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_A_DISCOUNT_DETAIL_PRE_1 DETAIL ON MASTER.OMS_DISCOUNT_DETAIL_ID = DETAIL.DISCOUNT_DETAIL_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_A_DISCOUNT_DETAIL_4")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_ID AS DISCOUNT_ID,
  EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  REASON_ID AS REASON_ID,
  DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  lkp_OMS_DISCOUNT_DETAIL_ID AS lkp_OMS_DISCOUNT_DETAIL_ID,
  lkp_OMS_COMPANY_ID AS lkp_OMS_COMPANY_ID,
  lkp_OMS_ENTITY_TYPE_ID AS lkp_OMS_ENTITY_TYPE_ID,
  lkp_OMS_ENTITY_ID AS lkp_OMS_ENTITY_ID,
  lkp_OMS_ENTITY_LINE_ID AS lkp_OMS_ENTITY_LINE_ID,
  lkp_OMS_DISCOUNT_TYPE_ID AS lkp_OMS_DISCOUNT_TYPE_ID,
  lkp_OMS_DISCOUNT_ID AS lkp_OMS_DISCOUNT_ID,
  lkp_OMS_EXT_DISCOUNT_ID AS lkp_OMS_EXT_DISCOUNT_ID,
  lkp_OMS_DISCOUNT_STATUS_ID AS lkp_OMS_DISCOUNT_STATUS_ID,
  lkp_OMS_REASON_ID AS lkp_OMS_REASON_ID,
  lkp_DISCOUNT_AMT AS lkp_DISCOUNT_AMT,
  lkp_DISCOUNT_VALUE1 AS lkp_DISCOUNT_VALUE1,
  lkp_MARK_FOR_DELETION_FLAG AS lkp_MARK_FOR_DELETION_FLAG,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_SOURCE_TYPE AS lkp_OMS_LAST_UPDATED_SOURCE_TYPE,
  lkp_OMS_LAST_UPDATED_SOURCE AS lkp_OMS_LAST_UPDATED_SOURCE,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  NULL AS OMS_LAST_UPDATED_SOURCE_TYPE,
  NULL AS OMS_LAST_UPDATED_SOURCE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_A_DISCOUNT_DETAIL_4
WHERE
  ISNULL(lkp_OMS_DISCOUNT_DETAIL_ID)
  OR (
    NOT ISNULL(lkp_OMS_DISCOUNT_DETAIL_ID)
    AND (
      IFF(ISNULL(COMPANY_ID), 9999, COMPANY_ID) <> IFF(
        ISNULL(lkp_OMS_COMPANY_ID),
        9999,
        lkp_OMS_COMPANY_ID
      )
      OR IFF(ISNULL(ENTITY_TYPE_ID), 99, ENTITY_TYPE_ID) <> IFF(
        ISNULL(lkp_OMS_ENTITY_TYPE_ID),
        99,
        lkp_OMS_ENTITY_TYPE_ID
      )
      OR IFF(ISNULL(ENTITY_ID), 9999, ENTITY_ID) <> IFF(ISNULL(lkp_OMS_ENTITY_ID), 9999, lkp_OMS_ENTITY_ID)
      OR IFF(ISNULL(ENTITY_LINE_ID), 9999, ENTITY_LINE_ID) <> IFF(
        ISNULL(lkp_OMS_ENTITY_LINE_ID),
        9999,
        lkp_OMS_ENTITY_LINE_ID
      )
      OR IFF(ISNULL(DISCOUNT_TYPE_ID), 99, DISCOUNT_TYPE_ID) <> IFF(
        ISNULL(lkp_OMS_DISCOUNT_TYPE_ID),
        99,
        lkp_OMS_DISCOUNT_TYPE_ID
      )
      OR IFF(ISNULL(DISCOUNT_ID), 9999, DISCOUNT_ID) <> IFF(
        ISNULL(lkp_OMS_DISCOUNT_ID),
        9999,
        lkp_OMS_DISCOUNT_ID
      )
      OR IFF(
        ISNULL(LTRIM(RTRIM(EXTERNAL_DISCOUNT_ID))),
        ' ',
        LTRIM(RTRIM(EXTERNAL_DISCOUNT_ID))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_EXT_DISCOUNT_ID))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_EXT_DISCOUNT_ID))
      )
      OR IFF(ISNULL(DISCOUNT_STATUS_ID), 99, DISCOUNT_STATUS_ID) <> IFF(
        ISNULL(lkp_OMS_DISCOUNT_STATUS_ID),
        99,
        lkp_OMS_DISCOUNT_STATUS_ID
      )
      OR IFF(ISNULL(REASON_ID), 9999, REASON_ID) <> IFF(ISNULL(lkp_OMS_REASON_ID), 9999, lkp_OMS_REASON_ID)
      OR IFF(ISNULL(DISCOUNT_AMOUNT), 9999, DISCOUNT_AMOUNT) <> IFF(ISNULL(lkp_DISCOUNT_AMT), 9999, lkp_DISCOUNT_AMT)
      OR IFF(ISNULL(DISCOUNT_VALUE), 9999, DISCOUNT_VALUE) <> IFF(
        ISNULL(lkp_DISCOUNT_VALUE1),
        9999,
        lkp_DISCOUNT_VALUE1
      )
      OR IFF(ISNULL(MARK_FOR_DELETION), 9, MARK_FOR_DELETION) <> IFF(
        ISNULL(lkp_MARK_FOR_DELETION_FLAG),
        9,
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
        ISNULL(OMS_LAST_UPDATED_SOURCE_TYPE),
        9999,
        OMS_LAST_UPDATED_SOURCE_TYPE
      ) <> IFF(
        ISNULL(lkp_OMS_LAST_UPDATED_SOURCE_TYPE),
        9999,
        lkp_OMS_LAST_UPDATED_SOURCE_TYPE
      )
      OR IFF(
        ISNULL(LTRIM(RTRIM(OMS_LAST_UPDATED_SOURCE))),
        ' ',
        LTRIM(RTRIM(OMS_LAST_UPDATED_SOURCE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_LAST_UPDATED_SOURCE))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_LAST_UPDATED_SOURCE))
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
  DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_ID AS DISCOUNT_ID,
  EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  REASON_ID AS REASON_ID,
  DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
  now() AS o_UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS o_LOAD_TSTMP,
  IFF(ISNULL(lkp_OMS_DISCOUNT_DETAIL_ID), 1, 2) AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_UPDATE_7


query_7 = f"""SELECT
  DISCOUNT_DETAIL_ID AS DISCOUNT_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_ID AS DISCOUNT_ID,
  EXTERNAL_DISCOUNT_ID AS EXTERNAL_DISCOUNT_ID,
  DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  REASON_ID AS REASON_ID,
  DISCOUNT_STATUS_ID AS DISCOUNT_STATUS_ID,
  DISCOUNT_VALUE AS DISCOUNT_VALUE,
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
# DBTITLE 1, OMS_A_DISCOUNT_DETAIL


spark.sql("""MERGE INTO OMS_A_DISCOUNT_DETAIL AS TARGET
USING
  UPD_INSERT_UPDATE_7 AS SOURCE ON TARGET.OMS_DISCOUNT_DETAIL_ID = SOURCE.DISCOUNT_DETAIL_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_DISCOUNT_DETAIL_ID = SOURCE.DISCOUNT_DETAIL_ID,
  TARGET.OMS_COMPANY_ID = SOURCE.COMPANY_ID,
  TARGET.OMS_ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID,
  TARGET.OMS_ENTITY_ID = SOURCE.ENTITY_ID,
  TARGET.OMS_ENTITY_LINE_ID = SOURCE.ENTITY_LINE_ID,
  TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID,
  TARGET.OMS_DISCOUNT_ID = SOURCE.DISCOUNT_ID,
  TARGET.OMS_EXT_DISCOUNT_ID = SOURCE.EXTERNAL_DISCOUNT_ID,
  TARGET.OMS_DISCOUNT_STATUS_ID = SOURCE.DISCOUNT_STATUS_ID,
  TARGET.OMS_REASON_ID = SOURCE.REASON_ID,
  TARGET.DISCOUNT_AMT = SOURCE.DISCOUNT_AMOUNT,
  TARGET.DISCOUNT_VALUE = SOURCE.DISCOUNT_VALUE,
  TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_SOURCE_TYPE = SOURCE.LAST_UPDATED_SOURCE_TYPE,
  TARGET.OMS_LAST_UPDATED_SOURCE = SOURCE.LAST_UPDATED_SOURCE,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_COMPANY_ID = SOURCE.COMPANY_ID
  AND TARGET.OMS_ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID
  AND TARGET.OMS_ENTITY_ID = SOURCE.ENTITY_ID
  AND TARGET.OMS_ENTITY_LINE_ID = SOURCE.ENTITY_LINE_ID
  AND TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID
  AND TARGET.OMS_DISCOUNT_ID = SOURCE.DISCOUNT_ID
  AND TARGET.OMS_EXT_DISCOUNT_ID = SOURCE.EXTERNAL_DISCOUNT_ID
  AND TARGET.OMS_DISCOUNT_STATUS_ID = SOURCE.DISCOUNT_STATUS_ID
  AND TARGET.OMS_REASON_ID = SOURCE.REASON_ID
  AND TARGET.DISCOUNT_AMT = SOURCE.DISCOUNT_AMOUNT
  AND TARGET.DISCOUNT_VALUE = SOURCE.DISCOUNT_VALUE
  AND TARGET.MARK_FOR_DELETION_FLAG = SOURCE.MARK_FOR_DELETION
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_SOURCE_TYPE = SOURCE.LAST_UPDATED_SOURCE_TYPE
  AND TARGET.OMS_LAST_UPDATED_SOURCE = SOURCE.LAST_UPDATED_SOURCE
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_DISCOUNT_DETAIL_ID,
    TARGET.OMS_COMPANY_ID,
    TARGET.OMS_ENTITY_TYPE_ID,
    TARGET.OMS_ENTITY_ID,
    TARGET.OMS_ENTITY_LINE_ID,
    TARGET.OMS_DISCOUNT_TYPE_ID,
    TARGET.OMS_DISCOUNT_ID,
    TARGET.OMS_EXT_DISCOUNT_ID,
    TARGET.OMS_DISCOUNT_STATUS_ID,
    TARGET.OMS_REASON_ID,
    TARGET.DISCOUNT_AMT,
    TARGET.DISCOUNT_VALUE,
    TARGET.MARK_FOR_DELETION_FLAG,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_SOURCE_TYPE,
    TARGET.OMS_LAST_UPDATED_SOURCE,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.DISCOUNT_DETAIL_ID,
    SOURCE.COMPANY_ID,
    SOURCE.ENTITY_TYPE_ID,
    SOURCE.ENTITY_ID,
    SOURCE.ENTITY_LINE_ID,
    SOURCE.DISCOUNT_TYPE_ID,
    SOURCE.DISCOUNT_ID,
    SOURCE.EXTERNAL_DISCOUNT_ID,
    SOURCE.DISCOUNT_STATUS_ID,
    SOURCE.REASON_ID,
    SOURCE.DISCOUNT_AMOUNT,
    SOURCE.DISCOUNT_VALUE,
    SOURCE.MARK_FOR_DELETION,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_SOURCE_TYPE,
    SOURCE.LAST_UPDATED_SOURCE,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Detail")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Discount_Detail", mainWorkflowId, parentName)