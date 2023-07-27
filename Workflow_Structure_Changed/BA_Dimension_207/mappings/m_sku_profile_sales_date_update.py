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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_profile_sales_date_update")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_profile_sales_date_update", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STX_GFTCRD_PRE1_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  TXN_KEY_GID AS TXN_KEY_GID,
  SEQ_NBR AS SEQ_NBR,
  VOID_TYPE_CD AS VOID_TYPE_CD,
  SITE_NBR AS SITE_NBR,
  COUNTRY_CD AS COUNTRY_CD,
  SKU_NBR AS SKU_NBR,
  UPC_ID AS UPC_ID,
  UPC_KEYED_FLAG AS UPC_KEYED_FLAG,
  GFTCRD_NBR AS GFTCRD_NBR,
  GFTCRD_KEYED_FLAG AS GFTCRD_KEYED_FLAG,
  GC_INVOICE_NBR AS GC_INVOICE_NBR,
  GC_AUTH_NBR AS GC_AUTH_NBR,
  GFTCRD_SALE_FLAG AS GFTCRD_SALE_FLAG,
  GFTCRD_RECHARGE_FLAG AS GFTCRD_RECHARGE_FLAG,
  SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
  GC_AMT AS GC_AMT,
  GC_COST AS GC_COST
FROM
  STX_GFTCRD_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STX_GFTCRD_PRE1_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STX_GFTCRD_PRE_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  TXN_KEY_GID AS TXN_KEY_GID,
  SEQ_NBR AS SEQ_NBR,
  VOID_TYPE_CD AS VOID_TYPE_CD,
  SITE_NBR AS SITE_NBR,
  COUNTRY_CD AS COUNTRY_CD,
  SKU_NBR AS SKU_NBR,
  UPC_ID AS UPC_ID,
  UPC_KEYED_FLAG AS UPC_KEYED_FLAG,
  GFTCRD_NBR AS GFTCRD_NBR,
  GFTCRD_KEYED_FLAG AS GFTCRD_KEYED_FLAG,
  GC_INVOICE_NBR AS GC_INVOICE_NBR,
  GC_AUTH_NBR AS GC_AUTH_NBR,
  GFTCRD_SALE_FLAG AS GFTCRD_SALE_FLAG,
  GFTCRD_RECHARGE_FLAG AS GFTCRD_RECHARGE_FLAG,
  SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
  GC_AMT AS GC_AMT,
  GC_COST AS GC_COST
FROM
  STX_GFTCRD_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_STX_GFTCRD_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UPC_2


query_2 = f"""SELECT
  UPC_ID AS UPC_ID,
  UPC_CD AS UPC_CD,
  UPC_ADD_DT AS UPC_ADD_DT,
  UPC_DELETE_DT AS UPC_DELETE_DT,
  UPC_REFRESH_DT AS UPC_REFRESH_DT,
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR
FROM
  UPC"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_UPC_2")

# COMMAND ----------
# DBTITLE 1, ASQ_JOIN_POS_SALES_PRE_AND_UPC_3


query_3 = f"""SELECT
  MIN(DAY_DT) AS MAX_DAY_DT,
  MAX(DAY_DT) AS MIN_DAY_DT,
  PRODUCT_ID AS PRODUCT_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      SUP.DAY_DT,
      Shortcut_to_UPC_2.PRODUCT_ID
    FROM
      STX_UPC_PRE SUP,
      Shortcut_to_UPC_2
    WHERE
      SUP.VOID_TYPE_CD = 'N'
      AND SUP.UPC_ID = Shortcut_to_UPC_2.UPC_ID
    UNION
    SELECT
      SGP.DAY_DT,
      Shortcut_to_UPC_2.PRODUCT_ID
    FROM
      Shortcut_to_STX_GFTCRD_PRE_1 SGP,
      Shortcut_to_UPC_2
    WHERE
      SGP.VOID_TYPE_CD = 'N'
      AND SGP.UPC_ID = Shortcut_to_UPC_2.UPC_ID
  ) ADVENTNET_ALIAS1
GROUP BY
  PRODUCT_ID"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("ASQ_JOIN_POS_SALES_PRE_AND_UPC_3")

# COMMAND ----------
# DBTITLE 1, LKP_PRODUCT_SALES_DATE_4


query_4 = f"""SELECT
  SP.PRODUCT_ID AS PRODUCT_ID,
  SP.FIRST_SALE_DT AS FIRST_SALE_DT,
  SP.LAST_SALE_DT AS LAST_SALE_DT,
  AJPSPAU3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_JOIN_POS_SALES_PRE_AND_UPC_3 AJPSPAU3
  LEFT JOIN SKU_PROFILE SP ON SP.PRODUCT_ID = AJPSPAU3.PRODUCT_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_PRODUCT_SALES_DATE_4")

# COMMAND ----------
# DBTITLE 1, EXP_CHECK_SALES_DATE_5


query_5 = f"""SELECT
  LPSD4.PRODUCT_ID AS PRODUCT_ID,
  IFF(
    ISNULL(LPSD4.FIRST_SALE_DT),
    AJPSPAU3.MIN_DAY_DT,
    IFF(
      AJPSPAU3.MIN_DAY_DT < LPSD4.FIRST_SALE_DT,
      AJPSPAU3.MIN_DAY_DT,
      LPSD4.FIRST_SALE_DT
    )
  ) AS OUT_FIRST_SALE_DT,
  IFF(
    ISNULL(LPSD4.LAST_SALE_DT),
    AJPSPAU3.MAX_DAY_DT,
    IFF(
      AJPSPAU3.MAX_DAY_DT > LPSD4.LAST_SALE_DT,
      AJPSPAU3.MAX_DAY_DT,
      LPSD4.LAST_SALE_DT
    )
  ) AS OUT_LAST_SALE_DT,
  LPSD4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_PRODUCT_SALES_DATE_4 LPSD4
  INNER JOIN ASQ_JOIN_POS_SALES_PRE_AND_UPC_3 AJPSPAU3 ON LPSD4.Monotonically_Increasing_Id = AJPSPAU3.Monotonically_Increasing_Id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_CHECK_SALES_DATE_5")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_PRODUCT_SALES_DATE_6


query_6 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  OUT_FIRST_SALE_DT AS FIRST_SALE_DT,
  OUT_LAST_SALE_DT AS LAST_SALE_DT,
  sessstarttime AS o_UPDATE_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(PRODUCT_ID), 'DD_REJECT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_CHECK_SALES_DATE_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("UPD_UPDATE_PRODUCT_SALES_DATE_6")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE


spark.sql("""MERGE INTO SKU_PROFILE AS TARGET
USING
  UPD_UPDATE_PRODUCT_SALES_DATE_6 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.UPDATE_DT = SOURCE.o_UPDATE_DT,
  TARGET.FIRST_SALE_DT = SOURCE.FIRST_SALE_DT,
  TARGET.LAST_SALE_DT = SOURCE.LAST_SALE_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.UPDATE_DT = SOURCE.o_UPDATE_DT
  AND TARGET.FIRST_SALE_DT = SOURCE.FIRST_SALE_DT
  AND TARGET.LAST_SALE_DT = SOURCE.LAST_SALE_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.UPDATE_DT,
    TARGET.FIRST_SALE_DT,
    TARGET.LAST_SALE_DT
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.o_UPDATE_DT,
    SOURCE.FIRST_SALE_DT,
    SOURCE.LAST_SALE_DT
  )""")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_RPT


spark.sql("""MERGE INTO SKU_PROFILE_RPT AS TARGET
USING
  UPD_UPDATE_PRODUCT_SALES_DATE_6 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.FIRST_SALE_DT = SOURCE.FIRST_SALE_DT,
  TARGET.LAST_SALE_DT = SOURCE.LAST_SALE_DT,
  TARGET.UPDATE_DT = SOURCE.o_UPDATE_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.FIRST_SALE_DT = SOURCE.FIRST_SALE_DT
  AND TARGET.LAST_SALE_DT = SOURCE.LAST_SALE_DT
  AND TARGET.UPDATE_DT = SOURCE.o_UPDATE_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.FIRST_SALE_DT,
    TARGET.LAST_SALE_DT,
    TARGET.UPDATE_DT
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.FIRST_SALE_DT,
    SOURCE.LAST_SALE_DT,
    SOURCE.o_UPDATE_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_profile_sales_date_update")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_profile_sales_date_update", mainWorkflowId, parentName)
