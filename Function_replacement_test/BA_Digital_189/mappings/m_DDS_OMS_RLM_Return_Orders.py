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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_RLM_Return_Orders")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_RLM_Return_Orders", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_0


query_0 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_RLM_RETURN_ORDERS_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_1


query_1 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_RLM_RETURN_ORDERS_2


query_2 = f"""SELECT
  OMS_RETURN_ORDERS_ID AS OMS_RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED_FLAG AS RETURN_ORDERS_XO_CREATED_FLAG,
  AUTOMATED_RMA_FLAG AS AUTOMATED_RMA_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_RLM_RETURN_ORDERS"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_RLM_RETURN_ORDERS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_3


query_3 = f"""SELECT
  OMS_RETURN_ORDERS_ID AS OMS_RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED_FLAG AS RETURN_ORDERS_XO_CREATED_FLAG,
  AUTOMATED_RMA_FLAG AS AUTOMATED_RMA_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_RLM_RETURN_ORDERS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_RLM_RETURN_ORDERS_4


query_4 = f"""SELECT
  DETAIL.RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  DETAIL.RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  DETAIL.IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  MASTER.OMS_RETURN_ORDERS_ID AS lkp_OMS_RETURN_ORDERS_ID,
  MASTER.RETURN_ORDERS_XO_CREATED_FLAG AS lkp_RETURN_ORDERS_XO_CREATED_FLAG,
  MASTER.AUTOMATED_RMA_FLAG AS lkp_AUTOMATED_RMA_FLAG,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_RLM_RETURN_ORDERS_PRE_1 DETAIL ON MASTER.OMS_RETURN_ORDERS_ID = DETAIL.RETURN_ORDERS_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_RLM_RETURN_ORDERS_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_REC_5


query_5 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  lkp_OMS_RETURN_ORDERS_ID AS lkp_OMS_RETURN_ORDERS_ID,
  lkp_RETURN_ORDERS_XO_CREATED_FLAG AS lkp_RETURN_ORDERS_XO_CREATED_FLAG,
  lkp_AUTOMATED_RMA_FLAG AS lkp_AUTOMATED_RMA_FLAG,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_RLM_RETURN_ORDERS_4
WHERE
  ISNULL(lkp_OMS_RETURN_ORDERS_ID)
  OR (
    NOT ISNULL(lkp_OMS_RETURN_ORDERS_ID)
    AND (
      IFF(
        ISNULL(RETURN_ORDERS_XO_CREATED),
        TO_INTEGER(999999999),
        RETURN_ORDERS_XO_CREATED
      ) <> IFF(
        ISNULL(lkp_RETURN_ORDERS_XO_CREATED_FLAG),
        TO_INTEGER(999999999),
        lkp_RETURN_ORDERS_XO_CREATED_FLAG
      )
      OR IFF(
        ISNULL(IS_AUTOMATED_RMA),
        TO_INTEGER(999999999),
        IS_AUTOMATED_RMA
      ) <> IFF(
        ISNULL(lkp_AUTOMATED_RMA_FLAG),
        TO_INTEGER(999999999),
        lkp_AUTOMATED_RMA_FLAG
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FTR_UNCHANGED_REC_5")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAG_6


query_6 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  lkp_OMS_RETURN_ORDERS_ID AS lkp_OMS_RETURN_ORDERS_ID,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_RETURN_ORDERS_ID), 1, 2) AS VALID_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_REC_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_VALID_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  VALID_FLAG AS VALID_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(VALID_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_RLM_RETURN_ORDERS


spark.sql("""MERGE INTO OMS_RLM_RETURN_ORDERS AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_RETURN_ORDERS_ID = SOURCE.RETURN_ORDERS_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_RETURN_ORDERS_ID = SOURCE.RETURN_ORDERS_ID,
  TARGET.RETURN_ORDERS_XO_CREATED_FLAG = SOURCE.RETURN_ORDERS_XO_CREATED,
  TARGET.AUTOMATED_RMA_FLAG = SOURCE.IS_AUTOMATED_RMA,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.RETURN_ORDERS_XO_CREATED_FLAG = SOURCE.RETURN_ORDERS_XO_CREATED
  AND TARGET.AUTOMATED_RMA_FLAG = SOURCE.IS_AUTOMATED_RMA
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_RETURN_ORDERS_ID,
    TARGET.RETURN_ORDERS_XO_CREATED_FLAG,
    TARGET.AUTOMATED_RMA_FLAG,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.RETURN_ORDERS_ID,
    SOURCE.RETURN_ORDERS_XO_CREATED,
    SOURCE.IS_AUTOMATED_RMA,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_RLM_Return_Orders")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_RLM_Return_Orders", mainWorkflowId, parentName)
