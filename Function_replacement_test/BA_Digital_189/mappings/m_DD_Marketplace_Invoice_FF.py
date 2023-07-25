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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_Marketplace_Invoice_FF")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_Marketplace_Invoice_FF", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_1


query_1 = f"""SELECT
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
  Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_1")

# COMMAND ----------
# DBTITLE 1, EXP_Source_2


query_2 = f"""SELECT
  TO_CHAR(TXN_DT, 'YYYY-MM-DD') AS o_TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  COMPANY_ID AS COMPANY_ID,
  LOYALTY_NBR AS LOYALTY_NBR,
  CURRENCY_CD AS CURRENCY_CD,
  TOTAL_SALES_AMT AS TOTAL_SALES_AMT,
  TOTAL_TAX_AMT AS TOTAL_TAX_AMT,
  TOTAL_EXCL_SALES_AMT AS TOTAL_EXCL_SALES_AMT,
  LOAD_TSTMP AS LOAD_TSTMP,
  {Tgt_Filename} || '_' || TO_CHAR(now(), 'YYYYMMDD') || '.csv' AS o_FileName,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_DD_MARKETPLACE_INVOICE_HIST_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Source_2")

# COMMAND ----------
# DBTITLE 1, FIL_Current_Bills_3


query_3 = f"""SELECT
  o_TXN_DT AS o_TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  COMPANY_ID AS COMPANY_ID,
  LOYALTY_NBR AS LOYALTY_NBR,
  CURRENCY_CD AS CURRENCY_CD,
  TOTAL_SALES_AMT AS TOTAL_SALES_AMT,
  TOTAL_TAX_AMT AS TOTAL_TAX_AMT,
  TOTAL_EXCL_SALES_AMT AS TOTAL_EXCL_SALES_AMT,
  LOAD_TSTMP AS LOAD_TSTMP,
  o_FileName AS o_FileName,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Source_2
WHERE
  TRUNC(LOAD_TSTMP) >= TRUNC(now())"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FIL_Current_Bills_3")

# COMMAND ----------
# DBTITLE 1, DD_Marketplace_PETM_Invoice_FF


spark.sql("""INSERT INTO
  DD_Marketplace_PETM_Invoice_FF
SELECT
  o_TXN_DT AS Transaction_Date,
  DD_DELIVERY_UUID AS DoorDash_Delivery_UUID,
  DASHPASS_ORDER_FLAG AS DashPass_Order_Flag,
  COMPANY_ID AS Company_Code,
  LOYALTY_NBR AS POS_Treats_ID,
  CURRENCY_CD AS Currency_Type,
  TOTAL_SALES_AMT AS Total_Sales_Amount,
  TOTAL_TAX_AMT AS Total_Sales_Tax_Amount,
  TOTAL_EXCL_SALES_AMT AS Total_Excluded_Sales_Amount,
  o_FileName AS FileName
FROM
  FIL_Current_Bills_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_Marketplace_Invoice_FF")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_Marketplace_Invoice_FF", mainWorkflowId, parentName)
