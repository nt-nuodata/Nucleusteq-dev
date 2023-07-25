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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Lpn_Detail")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Lpn_Detail", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_LPN_DETAIL_PRE_0


query_0 = f"""SELECT
  LPN_ID AS LPN_ID,
  LPN_DETAIL_ID AS LPN_DETAIL_ID,
  DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  ITEM_ID AS ITEM_ID,
  ASN_DTL_ID AS ASN_DTL_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  SHIPPED_QTY AS SHIPPED_QTY,
  ITEM_NAME AS ITEM_NAME,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_LPN_DETAIL_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_LPN_DETAIL_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_LPN_DETAIL_PRE_1


query_1 = f"""SELECT
  LPN_ID AS LPN_ID,
  LPN_DETAIL_ID AS LPN_DETAIL_ID,
  DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  ITEM_ID AS ITEM_ID,
  ASN_DTL_ID AS ASN_DTL_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  SHIPPED_QTY AS SHIPPED_QTY,
  ITEM_NAME AS ITEM_NAME,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_LPN_DETAIL_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_LPN_DETAIL_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_LPN_DETAIL_2


query_2 = f"""SELECT
  OMS_LPN_ID AS OMS_LPN_ID,
  OMS_LPN_DETAIL_ID AS OMS_LPN_DETAIL_ID,
  OMS_ASN_DTL_ID AS OMS_ASN_DTL_ID,
  OMS_DISTRIBUTION_ORDER_DTL_ID AS OMS_DISTRIBUTION_ORDER_DTL_ID,
  OMS_ITEM_ID AS OMS_ITEM_ID,
  OMS_ITEM_NAME AS OMS_ITEM_NAME,
  SHIPPED_QTY AS SHIPPED_QTY,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_LPN_DETAIL"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_LPN_DETAIL_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_LPN_DETAIL_3


query_3 = f"""SELECT
  OMS_LPN_ID AS OMS_LPN_ID,
  OMS_LPN_DETAIL_ID AS OMS_LPN_DETAIL_ID,
  OMS_ASN_DTL_ID AS OMS_ASN_DTL_ID,
  OMS_DISTRIBUTION_ORDER_DTL_ID AS OMS_DISTRIBUTION_ORDER_DTL_ID,
  OMS_ITEM_ID AS OMS_ITEM_ID,
  OMS_ITEM_NAME AS OMS_ITEM_NAME,
  SHIPPED_QTY AS SHIPPED_QTY,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_LPN_DETAIL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_LPN_DETAIL_3")

# COMMAND ----------
# DBTITLE 1, JNR_LPN_DETAIL_4


query_4 = f"""SELECT
  DETAIL.LPN_ID AS LPN_ID,
  DETAIL.LPN_DETAIL_ID AS LPN_DETAIL_ID,
  DETAIL.DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  DETAIL.ITEM_ID AS ITEM_ID,
  DETAIL.ASN_DTL_ID AS ASN_DTL_ID,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.SHIPPED_QTY AS SHIPPED_QTY,
  DETAIL.ITEM_NAME AS ITEM_NAME,
  MASTER.OMS_LPN_ID AS lkp_OMS_LPN_ID,
  MASTER.OMS_LPN_DETAIL_ID AS lkp_OMS_LPN_DETAIL_ID,
  MASTER.OMS_ASN_DTL_ID AS lkp_OMS_ASN_DTL_ID,
  MASTER.OMS_DISTRIBUTION_ORDER_DTL_ID AS lkp_OMS_DISTRIBUTION_ORDER_DTL_ID,
  MASTER.OMS_ITEM_ID AS lkp_OMS_ITEM_ID,
  MASTER.OMS_ITEM_NAME AS lkp_OMS_ITEM_NAME,
  MASTER.SHIPPED_QTY AS lkp_SHIPPED_QTY,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_TSTMP AS llkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_LPN_DETAIL_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_LPN_DETAIL_PRE_1 DETAIL ON MASTER.OMS_LPN_ID = DETAIL.LPN_ID
  AND MASTER.OMS_LPN_DETAIL_ID = DETAIL.LPN_DETAIL_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_LPN_DETAIL_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_REC_5


query_5 = f"""SELECT
  LPN_ID AS LPN_ID,
  LPN_DETAIL_ID AS LPN_DETAIL_ID,
  DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  ITEM_ID AS ITEM_ID,
  ASN_DTL_ID AS ASN_DTL_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  SHIPPED_QTY AS SHIPPED_QTY,
  ITEM_NAME AS ITEM_NAME,
  lkp_OMS_LPN_ID AS lkp_OMS_LPN_ID,
  lkp_OMS_LPN_DETAIL_ID AS lkp_OMS_LPN_DETAIL_ID,
  lkp_OMS_ASN_DTL_ID AS lkp_OMS_ASN_DTL_ID,
  lkp_OMS_DISTRIBUTION_ORDER_DTL_ID AS lkp_OMS_DISTRIBUTION_ORDER_DTL_ID,
  lkp_OMS_ITEM_ID AS lkp_OMS_ITEM_ID,
  lkp_OMS_ITEM_NAME AS lkp_OMS_ITEM_NAME,
  lkp_SHIPPED_QTY AS lkp_SHIPPED_QTY,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  llkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_LPN_DETAIL_4
WHERE
  ISNULL(lkp_OMS_LPN_DETAIL_ID)
  OR (
    NOT ISNULL(lkp_OMS_LPN_DETAIL_ID)
    AND (
      IFF(
        ISNULL(ASN_DTL_ID),
        TO_INTEGER(999999999),
        ASN_DTL_ID
      ) <> IFF(
        ISNULL(lkp_OMS_ASN_DTL_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ASN_DTL_ID
      )
      OR IFF(ISNULL(ITEM_ID), TO_INTEGER(999999999), ITEM_ID) <> IFF(
        ISNULL(lkp_OMS_ITEM_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ITEM_ID
      )
      OR IFF(
        ISNULL(ASN_DTL_ID),
        TO_INTEGER(999999999),
        ASN_DTL_ID
      ) <> IFF(
        ISNULL(lkp_OMS_ASN_DTL_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ASN_DTL_ID
      )
      OR IFF(
        ISNULL(DISTRIBUTION_ORDER_DTL_ID),
        TO_INTEGER(999999999),
        DISTRIBUTION_ORDER_DTL_ID
      ) <> IFF(
        ISNULL(lkp_OMS_DISTRIBUTION_ORDER_DTL_ID),
        TO_INTEGER(999999999),
        lkp_OMS_DISTRIBUTION_ORDER_DTL_ID
      )
      OR IFF(
        ISNULL(SHIPPED_QTY),
        TO_INTEGER(999999999),
        SHIPPED_QTY
      ) <> IFF(
        ISNULL(lkp_SHIPPED_QTY),
        TO_INTEGER(999999999),
        lkp_SHIPPED_QTY
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(ITEM_NAME))),
        ' ',
        LTRIM(RTRIM(ITEM_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_ITEM_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_ITEM_NAME))
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
        ISNULL(llkp_OMS_LAST_UPDATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        llkp_OMS_LAST_UPDATED_TSTMP
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FTR_UNCHANGED_REC_5")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAG_6


query_6 = f"""SELECT
  LPN_ID AS LPN_ID,
  LPN_DETAIL_ID AS LPN_DETAIL_ID,
  DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  ITEM_ID AS ITEM_ID,
  ASN_DTL_ID AS ASN_DTL_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  SHIPPED_QTY AS SHIPPED_QTY,
  ITEM_NAME AS ITEM_NAME,
  lkp_OMS_LPN_DETAIL_ID AS lkp_OMS_LPN_DETAIL_ID,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP_exp,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_LPN_DETAIL_ID), 1, 2) AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_REC_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_VALID_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  LPN_ID AS LPN_ID,
  LPN_DETAIL_ID AS LPN_DETAIL_ID,
  ASN_DTL_ID AS ASN_DTL_ID,
  DISTRIBUTION_ORDER_DTL_ID AS DISTRIBUTION_ORDER_DTL_ID,
  ITEM_ID AS ITEM_ID,
  ITEM_NAME AS ITEM_NAME,
  SHIPPED_QTY AS SHIPPED_QTY,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  UPDATE_TSTMP_exp AS UPDATE_TSTMP_exp,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  o_VALID_UPDATOR AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_VALID_UPDATOR, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_LPN_DETAIL


spark.sql("""MERGE INTO OMS_LPN_DETAIL AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_LPN_DETAIL_ID = SOURCE.LPN_DETAIL_ID
  AND TARGET.OMS_LPN_ID = SOURCE.LPN_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_LPN_ID = SOURCE.LPN_ID,
  TARGET.OMS_LPN_DETAIL_ID = SOURCE.LPN_DETAIL_ID,
  TARGET.OMS_ASN_DTL_ID = SOURCE.ASN_DTL_ID,
  TARGET.OMS_DISTRIBUTION_ORDER_DTL_ID = SOURCE.DISTRIBUTION_ORDER_DTL_ID,
  TARGET.OMS_ITEM_ID = SOURCE.ITEM_ID,
  TARGET.OMS_ITEM_NAME = SOURCE.ITEM_NAME,
  TARGET.SHIPPED_QTY = SOURCE.SHIPPED_QTY,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_ASN_DTL_ID = SOURCE.ASN_DTL_ID
  AND TARGET.OMS_DISTRIBUTION_ORDER_DTL_ID = SOURCE.DISTRIBUTION_ORDER_DTL_ID
  AND TARGET.OMS_ITEM_ID = SOURCE.ITEM_ID
  AND TARGET.OMS_ITEM_NAME = SOURCE.ITEM_NAME
  AND TARGET.SHIPPED_QTY = SOURCE.SHIPPED_QTY
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_LPN_ID,
    TARGET.OMS_LPN_DETAIL_ID,
    TARGET.OMS_ASN_DTL_ID,
    TARGET.OMS_DISTRIBUTION_ORDER_DTL_ID,
    TARGET.OMS_ITEM_ID,
    TARGET.OMS_ITEM_NAME,
    TARGET.SHIPPED_QTY,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.LPN_ID,
    SOURCE.LPN_DETAIL_ID,
    SOURCE.ASN_DTL_ID,
    SOURCE.DISTRIBUTION_ORDER_DTL_ID,
    SOURCE.ITEM_ID,
    SOURCE.ITEM_NAME,
    SOURCE.SHIPPED_QTY,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.UPDATE_TSTMP_exp,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Lpn_Detail")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Lpn_Detail", mainWorkflowId, parentName)