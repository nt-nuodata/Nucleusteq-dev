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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Order_LN_Status")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_OMS_Order_LN_Status", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_0


query_0 = f"""SELECT
  OMS_PURCHASE_ORDERS_LINE_STATUS AS OMS_PURCHASE_ORDERS_LINE_STATUS,
  OMS_PURCHASE_ORDERS_LINE_STATUS_DESC AS OMS_PURCHASE_ORDERS_LINE_STATUS_DESC,
  NOTE AS NOTE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_PURCHASE_ORDERS_LINE_STATUS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_1


query_1 = f"""SELECT
  OMS_PURCHASE_ORDERS_LINE_STATUS AS OMS_PURCHASE_ORDERS_LINE_STATUS,
  OMS_PURCHASE_ORDERS_LINE_STATUS_DESC AS OMS_PURCHASE_ORDERS_LINE_STATUS_DESC,
  NOTE AS NOTE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_1")

# COMMAND ----------
# DBTITLE 1, EXP_DATA_TYPE_2


query_2 = f"""SELECT
  to_integer(OMS_PURCHASE_ORDERS_LINE_STATUS) AS PURCHASE_ORDERS_LINE_STATUS,
  OMS_PURCHASE_ORDERS_LINE_STATUS_DESC AS DESCRIPTION,
  NOTE AS NOTE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_PURCHASE_ORDERS_LINE_STATUS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_DATA_TYPE_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LN_STATUS_3


query_3 = f"""SELECT
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  OMS_ORDER_LN_STATUS_DESC AS OMS_ORDER_LN_STATUS_DESC,
  NOTE AS NOTE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ORDER_LN_STATUS"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LN_STATUS_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDER_LN_STATUS_4


query_4 = f"""SELECT
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  OMS_ORDER_LN_STATUS_DESC AS OMS_ORDER_LN_STATUS_DESC,
  NOTE AS NOTE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDER_LN_STATUS_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDER_LN_STATUS_4")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_EDW_5


query_5 = f"""SELECT
  MASTER.PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  MASTER.DESCRIPTION AS DESCRIPTION,
  MASTER.NOTE AS NOTE,
  DETAIL.OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  DETAIL.OMS_ORDER_LN_STATUS_DESC AS OMS_ORDER_LN_STATUS_DESC,
  DETAIL.NOTE AS NOTE1,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DATA_TYPE_2 MASTER
  LEFT JOIN SQ_Shortcut_to_OMS_ORDER_LN_STATUS_4 DETAIL ON MASTER.PURCHASE_ORDERS_LINE_STATUS = DETAIL.OMS_ORDER_LN_STATUS_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("JNR_OMS_EDW_5")

# COMMAND ----------
# DBTITLE 1, EXP_FLAGS_6


query_6 = f"""SELECT
  PURCHASE_ORDERS_LINE_STATUS AS src_PURCHASE_ORDERS_LINE_STATUS,
  DESCRIPTION AS src_DESCRIPTION,
  NOTE AS src_NOTE,
  IFF(
    ISNULL(OMS_ORDER_LN_STATUS_ID),
    PURCHASE_ORDERS_LINE_STATUS,
    OMS_ORDER_LN_STATUS_ID
  ) AS PURCHASE_ORDERS_LINE_STATUS,
  IFF(
    ISNULL(OMS_ORDER_LN_STATUS_ID),
    'INSERT',
    IFF(
      NOT ISNULL(OMS_ORDER_LN_STATUS_ID)
      AND (
        LTRIM(RTRIM(IFF(ISNULL(DESCRIPTION), ' ', DESCRIPTION))) != LTRIM(
          RTRIM(
            IFF(
              ISNULL(OMS_ORDER_LN_STATUS_DESC),
              ' ',
              OMS_ORDER_LN_STATUS_DESC
            )
          )
        )
        OR LTRIM(RTRIM(IFF(ISNULL(NOTE), ' ', NOTE))) != LTRIM(RTRIM(IFF(ISNULL(NOTE1), ' ', NOTE1)))
      ),
      'UPDATE',
      'REJECT'
    )
  ) AS LOAD_FLAG,
  now() AS UPDATE_TSTMP,
  IFF(
    ISNULL(OMS_ORDER_LN_STATUS_ID),
    now(),
    LOAD_TSTMP
  ) AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_EDW_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_FLAGS_6")

# COMMAND ----------
# DBTITLE 1, FIL_FLAGS_7


query_7 = f"""SELECT
  PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  src_DESCRIPTION AS DESCRIPTION1,
  src_NOTE AS NOTE1,
  LOAD_FLAG AS LOAD_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_FLAGS_6
WHERE
  IN(LOAD_FLAG, 'INSERT', 'UPDATE')"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_FLAGS_7")

# COMMAND ----------
# DBTITLE 1, UPD_FLAGS_8


query_8 = f"""SELECT
  PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  DESCRIPTION1 AS DESCRIPTION1,
  NOTE1 AS NOTE1,
  LOAD_FLAG AS LOAD_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    LOAD_FLAG = 'INSERT',
    'DD_INSERT',
    IFF(LOAD_FLAG = 'UPDATE', 'DD_UPDATE', 'DD_REJECT')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  FIL_FLAGS_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("UPD_FLAGS_8")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_LN_STATUS


spark.sql("""MERGE INTO OMS_ORDER_LN_STATUS AS TARGET
USING
  UPD_FLAGS_8 AS SOURCE ON TARGET.OMS_ORDER_LN_STATUS_ID = SOURCE.PURCHASE_ORDERS_LINE_STATUS
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_ORDER_LN_STATUS_ID = SOURCE.PURCHASE_ORDERS_LINE_STATUS,
  TARGET.OMS_ORDER_LN_STATUS_DESC = SOURCE.DESCRIPTION1,
  TARGET.NOTE = SOURCE.NOTE1,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_ORDER_LN_STATUS_DESC = SOURCE.DESCRIPTION1
  AND TARGET.NOTE = SOURCE.NOTE1
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_ORDER_LN_STATUS_ID,
    TARGET.OMS_ORDER_LN_STATUS_DESC,
    TARGET.NOTE,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PURCHASE_ORDERS_LINE_STATUS,
    SOURCE.DESCRIPTION1,
    SOURCE.NOTE1,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Order_LN_Status")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_OMS_Order_LN_Status", mainWorkflowId, parentName)
