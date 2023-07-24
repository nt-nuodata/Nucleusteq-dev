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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Invoice_Hist_Upd")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_MarketPlace_Invoice_Hist_Upd", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, src_DD_MARKETPLACE_MTHLY_INVOICE_0


query_0 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  DD_MARKETPLACE_MTHLY_INVOICE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("src_DD_MARKETPLACE_MTHLY_INVOICE_0")

# COMMAND ----------
# DBTITLE 1, sq_DD_MARKETPLACE_MTHLY_INVOICE_1


query_1 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NBR AS INVOICE_NBR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_DD_MARKETPLACE_MTHLY_INVOICE_0
WHERE
  {SRC_FILTER}"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_DD_MARKETPLACE_MTHLY_INVOICE_1")

# COMMAND ----------
# DBTITLE 1, src_DD_MARKETPLACE_INVOICE_HIST_2


query_2 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  DD_INVOICE_NBR AS DD_INVOICE_NBR,
  COMPANY_ID AS COMPANY_ID,
  LOYALTY_NBR AS LOYALTY_NBR,
  CURRENCY_CD AS CURRENCY_CD,
  TOTAL_SALES_AMT AS TOTAL_SALES_AMT,
  TOTAL_TAX_AMT AS TOTAL_TAX_AMT,
  TOTAL_EXCL_SALES_AMT AS TOTAL_EXCL_SALES_AMT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  DD_MARKETPLACE_INVOICE_HIST"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("src_DD_MARKETPLACE_INVOICE_HIST_2")

# COMMAND ----------
# DBTITLE 1, sq_DD_MARKETPLACE_INVOICE_HIST_3


query_3 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  DD_INVOICE_NBR AS DD_INVOICE_NBR,
  COMPANY_ID AS COMPANY_ID,
  LOYALTY_NBR AS LOYALTY_NBR,
  CURRENCY_CD AS CURRENCY_CD,
  TOTAL_SALES_AMT AS TOTAL_SALES_AMT,
  TOTAL_TAX_AMT AS TOTAL_TAX_AMT,
  TOTAL_EXCL_SALES_AMT AS TOTAL_EXCL_SALES_AMT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_DD_MARKETPLACE_INVOICE_HIST_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("sq_DD_MARKETPLACE_INVOICE_HIST_3")

# COMMAND ----------
# DBTITLE 1, jnr_SRC_TGT_4


query_4 = f"""SELECT
  MASTER.TXN_DT AS src_TXN_DT,
  MASTER.DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID,
  MASTER.DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  MASTER.INVOICE_NBR AS src_INVOICE_NBR,
  DETAIL.TXN_DT AS tgt_TXN_DT,
  DETAIL.DD_DELIVERY_UUID AS tgt_DD_DELIVERY_UUID,
  DETAIL.DD_INVOICE_NBR AS tgt_DD_INVOICE_NBR,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_DD_MARKETPLACE_MTHLY_INVOICE_1 MASTER
  LEFT JOIN sq_DD_MARKETPLACE_INVOICE_HIST_3 DETAIL ON MASTER.TXN_DT = DETAIL.TXN_DT
  AND MASTER.DD_DELIVERY_UUID = DETAIL.DD_DELIVERY_UUID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("jnr_SRC_TGT_4")

# COMMAND ----------
# DBTITLE 1, exp_Flag_5


query_5 = f"""SELECT
  src_TXN_DT AS src_TXN_DT,
  src_DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG,
  src_INVOICE_NBR AS src_INVOICE_NBR,
  IFF(
    ISNULL(tgt_DD_DELIVERY_UUID),
    'I',
    IFF (
      NOT ISNULL(tgt_DD_DELIVERY_UUID)
      AND (
        IFF (ISNULL(src_INVOICE_NBR), 1, src_INVOICE_NBR) <> IFF (ISNULL(tgt_DD_INVOICE_NBR), 1, tgt_DD_INVOICE_NBR)
      ),
      'U',
      'X'
    )
  ) AS o_FLAG,
  SESSSTARTTIME AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_SRC_TGT_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("exp_Flag_5")

# COMMAND ----------
# DBTITLE 1, rtr_UPD_INVOICE_OR_LOAD_FF


query_6 = f"""SELECT
  src_TXN_DT AS src_TXN_DT1,
  src_DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID1,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG1,
  src_INVOICE_NBR AS src_INVOICE_NBR1,
  o_FLAG AS o_FLAG1,
  o_LOAD_TSTMP AS o_LOAD_TSTMP1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_Flag_5
WHERE
  o_FLAG = 'U'"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("FIL_UPD_INVOICE_OR_LOAD_FF_UPD_INVOICE_NBR_6")

query_7 = f"""SELECT
  src_TXN_DT AS src_TXN_DT3,
  src_DD_DELIVERY_UUID AS src_DD_DELIVERY_UUID3,
  src_DASHPASS_ORDER_FLAG AS src_DASHPASS_ORDER_FLAG3,
  src_INVOICE_NBR AS src_INVOICE_NBR3,
  o_FLAG AS o_FLAG3,
  o_LOAD_TSTMP AS o_LOAD_TSTMP3,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_Flag_5
WHERE
  o_FLAG = 'I'"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_UPD_INVOICE_OR_LOAD_FF_LOAD_FF_7")

# COMMAND ----------
# DBTITLE 1, exp_ERR_CNT_8


query_8 = f"""SELECT
  v_TXN_CNT + 1 AS v_TXN_CNT,
  SETVARIABLE('TXN_CNT', v_TXN_CNT + 1) AS o_TXN_CNT,
  src_TXN_DT3 AS src_TXN_DT3,
  src_DD_DELIVERY_UUID3 AS src_DD_DELIVERY_UUID3,
  src_DASHPASS_ORDER_FLAG3 AS src_DASHPASS_ORDER_FLAG3,
  src_INVOICE_NBR3 AS src_INVOICE_NBR3,
  o_LOAD_TSTMP3 AS o_LOAD_TSTMP3,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UPD_INVOICE_OR_LOAD_FF_LOAD_FF_7"""

df_8 = spark.sql(query_8)

if df_8.count() > 0 :
  TXN_CNT = df_8.agg({'o_TXN_CNT' : 'max'}).collect()[0][0]

df_8.createOrReplaceTempView("exp_ERR_CNT_8")

# COMMAND ----------
# DBTITLE 1, exp_SCRUB_9


query_9 = f"""SELECT
  src_TXN_DT3 AS src_TXN_DT3,
  src_DD_DELIVERY_UUID3 AS src_DD_DELIVERY_UUID3,
  src_DASHPASS_ORDER_FLAG3 AS src_DASHPASS_ORDER_FLAG3,
  src_INVOICE_NBR3 AS src_INVOICE_NBR3,
  o_LOAD_TSTMP3 AS o_LOAD_TSTMP3,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_ERR_CNT_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("exp_SCRUB_9")

# COMMAND ----------
# DBTITLE 1, upd_TGT_10


query_10 = f"""SELECT
  src_TXN_DT1 AS TXN_DT,
  src_DD_DELIVERY_UUID1 AS DD_DELIVERY_UUID,
  src_INVOICE_NBR1 AS INVOICE_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  0 AS UPDATE_STRATEGY_FLAG
FROM
  FIL_UPD_INVOICE_OR_LOAD_FF_UPD_INVOICE_NBR_6"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("upd_TGT_10")

# COMMAND ----------
# DBTITLE 1, DD_PETM_EXTRA_DELIVERY_ID


spark.sql("""INSERT INTO
  DD_PETM_EXTRA_DELIVERY_ID
SELECT
  src_TXN_DT3 AS TXN_DT,
  src_DD_DELIVERY_UUID3 AS DD_DELIVERY_UUID,
  src_DASHPASS_ORDER_FLAG3 AS DASHPASS_ORDER_FLAG,
  src_INVOICE_NBR3 AS INVOICE_NUMBER,
  o_LOAD_TSTMP3 AS LOAD_TSTMP
FROM
  exp_SCRUB_9""")

# COMMAND ----------
# DBTITLE 1, DD_MARKETPLACE_INVOICE_HIST


spark.sql("""MERGE INTO DD_MARKETPLACE_INVOICE_HIST AS TARGET
USING
  upd_TGT_10 AS SOURCE ON TARGET.TXN_DT = SOURCE.TXN_DT
  AND TARGET.DD_DELIVERY_UUID = SOURCE.DD_DELIVERY_UUID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.TXN_DT = SOURCE.TXN_DT,
  TARGET.DD_DELIVERY_UUID = SOURCE.DD_DELIVERY_UUID,
  TARGET.DD_INVOICE_NBR = SOURCE.INVOICE_NBR
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.DD_INVOICE_NBR = SOURCE.INVOICE_NBR THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.TXN_DT,
    TARGET.DD_DELIVERY_UUID,
    TARGET.DD_INVOICE_NBR
  )
VALUES
  (
    SOURCE.TXN_DT,
    SOURCE.DD_DELIVERY_UUID,
    SOURCE.INVOICE_NBR
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Invoice_Hist_Upd")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_MarketPlace_Invoice_Hist_Upd", mainWorkflowId, parentName)
