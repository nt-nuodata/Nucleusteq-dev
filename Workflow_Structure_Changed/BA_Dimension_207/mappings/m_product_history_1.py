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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_history_1")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_product_history_1", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_PRODUCT_KEY_PRE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  BUYER_ID AS BUYER_ID,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  ITEM_CONCATENATED AS ITEM_CONCATENATED
FROM
  PRODUCT_KEY_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_PRODUCT_KEY_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_PRODUCT_HISTORY_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  BUYER_ID AS BUYER_ID,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  ITEM_CONCATENATED AS ITEM_CONCATENATED
FROM
  PRODUCT_HISTORY"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_PRODUCT_HISTORY_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_PRODUCT_HISTORY_2


query_2 = f"""SELECT
  PROD.PRODUCT_ID AS PRODUCT_ID,
  CURRENT_DATE AS PROD_HIST_EFF_DT,
  TO_DATE('9999-12-31', 'YYYY-MM-DD') AS PROD_HIST_END_DT,
  PROD.BUYER_ID AS BUYER_ID,
  PROD.PRIMARY_UPC AS PRIMARY_UPC,
  PROD.PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PROD.PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PROD.SKU_NBR AS SKU_NBR,
  PROD.SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  PROD.OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  PROD.ITEM_CONCATENATED AS ITEM_CONCATENATED,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_PRODUCT_KEY_PRE_0 PROD,
  Shortcut_To_PRODUCT_HISTORY_1 HIST
WHERE
  HIST.PROD_HIST_END_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
  AND PROD.PRODUCT_ID = HIST.PRODUCT_ID
  AND (
    PROD.BUYER_ID <> HIST.BUYER_ID
    OR PROD.PRIMARY_UPC <> HIST.PRIMARY_UPC
    OR PROD.PRIMARY_VENDOR_ID <> HIST.PRIMARY_VENDOR_ID
    OR PROD.PURCH_GROUP_ID <> HIST.PURCH_GROUP_ID
    OR PROD.SKU_NBR <> HIST.SKU_NBR
    OR PROD.SAP_CATEGORY_ID <> HIST.SAP_CATEGORY_ID
    OR PROD.OLD_ARTICLE_NBR <> NVL(HIST.OLD_ARTICLE_NBR, ' ')
    OR PROD.ITEM_CONCATENATED <> NVL(HIST.ITEM_CONCATENATED, ' ')
  )"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_To_PRODUCT_HISTORY_2")

# COMMAND ----------
# DBTITLE 1, PRODUCT_HISTORY


spark.sql("""INSERT INTO
  PRODUCT_HISTORY
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  BUYER_ID AS BUYER_ID,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  ITEM_CONCATENATED AS ITEM_CONCATENATED
FROM
  ASQ_Shortcut_To_PRODUCT_HISTORY_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_history_1")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_product_history_1", mainWorkflowId, parentName)
