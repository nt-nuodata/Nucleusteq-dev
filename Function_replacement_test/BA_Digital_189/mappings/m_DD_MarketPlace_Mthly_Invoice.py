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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invoice")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_MarketPlace_Mthly_Invoice", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, src_DD_MARKETPLACE_MTHLY_INVOICE_PRE_0


query_0 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  DD_MARKETPLACE_MTHLY_INVOICE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("src_DD_MARKETPLACE_MTHLY_INVOICE_PRE_0")

# COMMAND ----------
# DBTITLE 1, sq_DD_MARKETPLACE_MTHLY_INVOICE_PRE_1


query_1 = f"""SELECT
  DISTINCT TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_DD_MARKETPLACE_MTHLY_INVOICE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_DD_MARKETPLACE_MTHLY_INVOICE_PRE_1")

# COMMAND ----------
# DBTITLE 1, src_DD_MARKETPLACE_MTHLY_INVOICE_2


query_2 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  DD_MARKETPLACE_MTHLY_INVOICE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("src_DD_MARKETPLACE_MTHLY_INVOICE_2")

# COMMAND ----------
# DBTITLE 1, sq_DD_MARKETPLACE_MTHLY_INVOICE_3


query_3 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_DD_MARKETPLACE_MTHLY_INVOICE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("sq_DD_MARKETPLACE_MTHLY_INVOICE_3")

# COMMAND ----------
# DBTITLE 1, jnr_SRC_TGT_4


query_4 = f"""SELECT
  MASTER.TXN_DT AS src_TXN_DT,
  MASTER.DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID,
  MASTER.DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  MASTER.INVOICE_NBR AS src_INVOICE_NBR,
  DETAIL.TXN_DT AS tgt_TXN_DT,
  DETAIL.DD_DELIVERY_UUID AS tgt_DD_DELIVERY_UUID,
  DETAIL.DASHPASS_ORDER_FLAG AS tgt_DASHPASS_ORDER_FLAG,
  DETAIL.INVOICE_NBR AS tgt_INVOICE_NBR,
  DETAIL.LOAD_TSTMP AS tgt_LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_DD_MARKETPLACE_MTHLY_INVOICE_PRE_1 MASTER
  LEFT JOIN sq_DD_MARKETPLACE_MTHLY_INVOICE_3 DETAIL ON MASTER.TXN_DT = DETAIL.TXN_DT
  AND MASTER.DD_DELIVERY_UUID = DETAIL.DD_DELIVERY_UUID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("jnr_SRC_TGT_4")

# COMMAND ----------
# DBTITLE 1, exp_TARGET_5


query_5 = f"""SELECT
  src_TXN_DT AS src_TXN_DT,
  src_DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  src_INVOICE_NBR AS src_INVOICE_NBR,
  SESSSTARTTIME AS o_UPDATE_TSTMP,
  IFF(
    ISNULL(tgt_LOAD_TSTMP),
    SESSSTARTTIME,
    tgt_LOAD_TSTMP
  ) AS o_LOAD_TSTMP,
  IFF(
    ISNULL(tgt_LOAD_TSTMP),
    'I',
    IFF (
      NOT ISNULL(tgt_LOAD_TSTMP)
      AND (
        (
          IFF(
            ISNULL(src_DASHPASS_ORDER_FLAG),
            1,
            src_DASHPASS_ORDER_FLAG
          ) <> IFF(
            ISNULL(tgt_DASHPASS_ORDER_FLAG),
            1,
            tgt_DASHPASS_ORDER_FLAG
          )
        )
        OR IFF (ISNULL(src_INVOICE_NBR), 1, src_INVOICE_NBR) <> IFF (ISNULL(tgt_INVOICE_NBR), 1, tgt_INVOICE_NBR)
      ),
      'U',
      'X'
    )
  ) AS o_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_SRC_TGT_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("exp_TARGET_5")

# COMMAND ----------
# DBTITLE 1, fil_NO_CHANGE_6


query_6 = f"""SELECT
  src_TXN_DT AS src_TXN_DT,
  src_DD_DELIVERY_UUID AS src_DD_DELIVERY_ID,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  src_INVOICE_NBR AS src_INVOICE_NBR,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  o_FLAG AS o_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_TARGET_5
WHERE
  o_FLAG <> 'X'"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("fil_NO_CHANGE_6")

# COMMAND ----------
# DBTITLE 1, upd_INS_UPD_7


query_7 = f"""SELECT
  src_TXN_DT AS src_TXN_DT,
  src_DD_DELIVERY_ID AS src_DD_DELIVERY_UUID,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  src_INVOICE_NBR AS src_INVOICE_NBR,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  o_FLAG AS o_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_FLAG, 'I', 'DD_INSERT', 'U', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  fil_NO_CHANGE_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("upd_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, DD_MARKETPLACE_MTHLY_INVOICE


spark.sql("""MERGE INTO DD_MARKETPLACE_MTHLY_INVOICE AS TARGET
USING
  upd_INS_UPD_7 AS SOURCE ON TARGET.TXN_DT = SOURCE.src_TXN_DT
  AND TARGET.DD_DELIVERY_UUID = SOURCE.src_DD_DELIVERY_UUID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.TXN_DT = SOURCE.src_TXN_DT,
  TARGET.DD_DELIVERY_UUID = SOURCE.src_DD_DELIVERY_UUID,
  TARGET.DASHPASS_ORDER_FLAG = SOURCE.src_DASHPASS_ORDER_FLAG,
  TARGET.INVOICE_NBR = SOURCE.src_INVOICE_NBR,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.DASHPASS_ORDER_FLAG = SOURCE.src_DASHPASS_ORDER_FLAG
  AND TARGET.INVOICE_NBR = SOURCE.src_INVOICE_NBR
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.TXN_DT,
    TARGET.DD_DELIVERY_UUID,
    TARGET.DASHPASS_ORDER_FLAG,
    TARGET.INVOICE_NBR,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.src_TXN_DT,
    SOURCE.src_DD_DELIVERY_UUID,
    SOURCE.src_DASHPASS_ORDER_FLAG,
    SOURCE.src_INVOICE_NBR,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invoice")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_MarketPlace_Mthly_Invoice", mainWorkflowId, parentName)
