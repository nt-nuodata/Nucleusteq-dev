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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_prod_sales_date_update")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_prod_sales_date_update", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STX_UPC_PRE_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  TXN_KEY_GID AS TXN_KEY_GID,
  UPC_ID AS UPC_ID,
  SEQ_NBR AS SEQ_NBR,
  VOID_TYPE_CD AS VOID_TYPE_CD,
  TXN_TYPE_ID AS TXN_TYPE_ID,
  ADOPTION_GROUP_ID AS ADOPTION_GROUP_ID,
  COMBO_TYPE_CD AS COMBO_TYPE_CD,
  PARENT_UPC_ID AS PARENT_UPC_ID,
  SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
  UPC_KEYED_FLAG AS UPC_KEYED_FLAG,
  UPC_NOTONFILE_FLAG AS UPC_NOTONFILE_FLAG,
  ADJ_REASON_ID AS ADJ_REASON_ID,
  ORIG_UPC_TAX_STATUS_ID AS ORIG_UPC_TAX_STATUS_ID,
  REGULATED_ANIMAL_PERMIT_NBR AS REGULATED_ANIMAL_PERMIT_NBR,
  CARE_SHEET_GIVEN_FLAG AS CARE_SHEET_GIVEN_FLAG,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_DESC AS RETURN_DESC,
  SPECIAL_ORDER_NBR AS SPECIAL_ORDER_NBR,
  TP_INVOICE_NBR AS TP_INVOICE_NBR,
  TP_MASTER_INVOICE_NBR AS TP_MASTER_INVOICE_NBR,
  TRAINING_START_DT AS TRAINING_START_DT,
  NON_TAX_FLAG AS NON_TAX_FLAG,
  NON_DISCOUNT_FLAG AS NON_DISCOUNT_FLAG,
  SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
  UPC_SEQ_NBR AS UPC_SEQ_NBR,
  UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
  SALES_AMT AS SALES_AMT,
  SALES_QTY AS SALES_QTY,
  SALES_COST AS SALES_COST,
  RETURN_AMT AS RETURN_AMT,
  RETURN_QTY AS RETURN_QTY,
  RETURN_COST AS RETURN_COST,
  SERVICE_AMT AS SERVICE_AMT,
  DROP_SHIP_FLAG AS DROP_SHIP_FLAG,
  PET_ID AS PET_ID,
  SERVICE_BULK_SKU_NBR AS SERVICE_BULK_SKU_NBR
FROM
  STX_UPC_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STX_UPC_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UPC_1


query_1 = f"""SELECT
  UPC_ID AS UPC_ID,
  UPC_CD AS UPC_CD,
  UPC_ADD_DT AS UPC_ADD_DT,
  UPC_DELETE_DT AS UPC_DELETE_DT,
  UPC_REFRESH_DT AS UPC_REFRESH_DT,
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR
FROM
  UPC"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_UPC_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Join_STX_UPC_PRE_And_Upc_2


query_2 = f"""SELECT
  MAX(SUP.DAY_DT) AS DAY_DT,
  Shortcut_to_UPC_1.PRODUCT_ID AS PRODUCT_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_STX_UPC_PRE_0 SUP,
  Shortcut_to_UPC_1
WHERE
  SUP.VOID_TYPE_CD = 'N'
  AND SUP.UPC_ID = Shortcut_to_UPC_1.UPC_ID
GROUP BY
  Shortcut_to_UPC_1.PRODUCT_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Join_STX_UPC_PRE_And_Upc_2")

# COMMAND ----------
# DBTITLE 1, LKP_PRODUCT_SALES_DATE_3


query_3 = f"""SELECT
  P.PRODUCT_ID AS PRODUCT_ID,
  P.DATE_FIRST_SALE AS DATE_FIRST_SALE,
  P.DATE_LAST_SALE AS DATE_LAST_SALE,
  AJSUPAU2.PRODUCT_ID AS IN_PRODUCT_ID,
  AJSUPAU2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Join_STX_UPC_PRE_And_Upc_2 AJSUPAU2
  LEFT JOIN PRODUCT P ON P.PRODUCT_ID = AJSUPAU2.PRODUCT_ID"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("LKP_PRODUCT_SALES_DATE_3")

# COMMAND ----------
# DBTITLE 1, EXP_CHECK_SALES_DATE_4


query_4 = f"""SELECT
  LPSD3.PRODUCT_ID AS PRODUCT_ID,
  IFF(
    AJSUPAU2.DAY_DT < LPSD3.DATE_FIRST_SALE,
    AJSUPAU2.DAY_DT,
    LPSD3.DATE_FIRST_SALE
  ) AS OUT_DATE_FIRST_SALE,
  IFF(
    AJSUPAU2.DAY_DT > LPSD3.DATE_LAST_SALE,
    AJSUPAU2.DAY_DT,
    LPSD3.DATE_LAST_SALE
  ) AS OUT_DATE_LAST_SALE,
  LPSD3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_PRODUCT_SALES_DATE_3 LPSD3
  INNER JOIN ASQ_Join_STX_UPC_PRE_And_Upc_2 AJSUPAU2 ON LPSD3.Monotonically_Increasing_Id = AJSUPAU2.Monotonically_Increasing_Id"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_CHECK_SALES_DATE_4")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_PRODUCT_SALES_DATE_5


query_5 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  OUT_DATE_FIRST_SALE AS DATE_FIRST_SALE,
  OUT_DATE_LAST_SALE AS DATE_LAST_SALE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(PRODUCT_ID), 'DD_REJECT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_CHECK_SALES_DATE_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("UPD_UPDATE_PRODUCT_SALES_DATE_5")

# COMMAND ----------
# DBTITLE 1, PRODUCT


spark.sql("""MERGE INTO PRODUCT AS TARGET
USING
  UPD_UPDATE_PRODUCT_SALES_DATE_5 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.DATE_FIRST_SALE = SOURCE.DATE_FIRST_SALE,
  TARGET.DATE_LAST_SALE = SOURCE.DATE_LAST_SALE
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.DATE_FIRST_SALE = SOURCE.DATE_FIRST_SALE
  AND TARGET.DATE_LAST_SALE = SOURCE.DATE_LAST_SALE THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.DATE_FIRST_SALE,
    TARGET.DATE_LAST_SALE
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.DATE_FIRST_SALE,
    SOURCE.DATE_LAST_SALE
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_prod_sales_date_update")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_prod_sales_date_update", mainWorkflowId, parentName)
