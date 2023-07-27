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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_payment_term")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_payment_term", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Payment_Terms_0


query_0 = f"""SELECT
  PaymentTermCd AS PaymentTermCd,
  PaymentTermDesc AS PaymentTermDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  Payment_Terms"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Payment_Terms_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Payment_Terms_1


query_1 = f"""SELECT
  PaymentTermCd AS PaymentTermCd,
  PaymentTermDesc AS PaymentTermDesc,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Payment_Terms_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Payment_Terms_1")

# COMMAND ----------
# DBTITLE 1, LKPTRANS_2


query_2 = f"""SELECT
  VPT.PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
  VPT.PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
  SStPT1.PaymentTermCd AS PaymentTermCd,
  SStPT1.PaymentTermDesc AS PaymentTermDesc,
  SStPT1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Payment_Terms_1 SStPT1
  LEFT JOIN VENDOR_PAYMENT_TERM VPT ON VPT.PAYMENT_TERM_CD = SStPT1.PaymentTermCd"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKPTRANS_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


query_3 = f"""SELECT
  PaymentTermCd AS PaymentTermCd,
  PaymentTermDesc AS PaymentTermDesc,
  IFF(PaymentTermDesc = '', 'UnKnown', PaymentTermDesc) AS PaymentTermDesc_Default,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKPTRANS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_4


query_4 = f"""SELECT
  L2.PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
  E3.PaymentTermCd AS PaymentTermCd,
  E3.PaymentTermDesc_Default AS PaymentTermDesc,
  L2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    isnull(L2.PAYMENT_TERM_CD),
    'DD_INSERT',
    'DD_UPDATE'
  ) AS UPDATE_STRATEGY_FLAG
FROM
  LKPTRANS_2 L2
  INNER JOIN EXPTRANS_3 E3 ON L2.Monotonically_Increasing_Id = E3.Monotonically_Increasing_Id"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("UPDTRANS_4")

# COMMAND ----------
# DBTITLE 1, VENDOR_PAYMENT_TERM


spark.sql("""MERGE INTO VENDOR_PAYMENT_TERM AS TARGET
USING
  UPDTRANS_4 AS SOURCE ON TARGET.PAYMENT_TERM_CD = SOURCE.PaymentTermCd
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PAYMENT_TERM_CD = SOURCE.PaymentTermCd,
  TARGET.PAYMENT_TERM_DESC = SOURCE.PaymentTermDesc
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PAYMENT_TERM_DESC = SOURCE.PaymentTermDesc THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (TARGET.PAYMENT_TERM_CD, TARGET.PAYMENT_TERM_DESC)
VALUES
  (SOURCE.PaymentTermCd, SOURCE.PaymentTermDesc)""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_payment_term")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_payment_term", mainWorkflowId, parentName)
