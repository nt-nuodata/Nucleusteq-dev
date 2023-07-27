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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_history_2")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_product_history_2", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, ASQ_PRODUCT_HISTORY_JOIN_PRODUCT_2


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
  Shortcut_To_PRODUCT_KEY_PRE_0 PROD
WHERE
  NOT EXISTS (
    SELECT
      HIST.PRODUCT_ID
    FROM
      Shortcut_To_PRODUCT_HISTORY_1 HIST
    WHERE
      PROD.PRODUCT_ID = HIST.PRODUCT_ID
      AND HIST.PROD_HIST_END_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
  )"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_PRODUCT_HISTORY_JOIN_PRODUCT_2")

# COMMAND ----------
# DBTITLE 1, PRODUCT_HISTORY


spark.sql("""INSERT INTO
  PRODUCT_HISTORY
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PRODUCT_ID AS PRODUCT_ID,
  PRODUCT_ID AS PRODUCT_ID,
  PRODUCT_ID AS PRODUCT_ID,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_EFF_DT AS PROD_HIST_EFF_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  PROD_HIST_END_DT AS PROD_HIST_END_DT,
  BUYER_ID AS BUYER_ID,
  BUYER_ID AS BUYER_ID,
  BUYER_ID AS BUYER_ID,
  BUYER_ID AS BUYER_ID,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SKU_NBR AS SKU_NBR,
  SKU_NBR AS SKU_NBR,
  SKU_NBR AS SKU_NBR,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  ITEM_CONCATENATED AS ITEM_CONCATENATED,
  ITEM_CONCATENATED AS ITEM_CONCATENATED,
  ITEM_CONCATENATED AS ITEM_CONCATENATED,
  ITEM_CONCATENATED AS ITEM_CONCATENATED
FROM
  ASQ_PRODUCT_HISTORY_JOIN_PRODUCT_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_history_2")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_product_history_2", mainWorkflowId, parentName)
