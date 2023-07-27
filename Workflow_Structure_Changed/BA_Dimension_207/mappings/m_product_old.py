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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_old")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_product_old", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, PRODUCT_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  ALT_DESC AS ALT_DESC,
  ANIMAL AS ANIMAL,
  ANIMALSIZE AS ANIMALSIZE,
  BRAND_NAME AS BRAND_NAME,
  BUM_QTY AS BUM_QTY,
  BUYER_ID AS BUYER_ID,
  BUYER_NAME AS BUYER_NAME,
  CTRY_ORIGIN AS CTRY_ORIGIN,
  DATE_DISC_START AS DATE_DISC_START,
  DATE_DISC_END AS DATE_DISC_END,
  DATE_FIRST_INV AS DATE_FIRST_INV,
  DATE_FIRST_SALE AS DATE_FIRST_SALE,
  DATE_LAST_INV AS DATE_LAST_INV,
  DATE_LAST_SALE AS DATE_LAST_SALE,
  DATE_PROD_ADDED AS DATE_PROD_ADDED,
  DATE_PROD_DELETED AS DATE_PROD_DELETED,
  DATE_PROD_REFRESHED AS DATE_PROD_REFRESHED,
  DEPTH AS DEPTH,
  DIM_UNITS AS DIM_UNITS,
  FLAVOR AS FLAVOR,
  HEIGHT AS HEIGHT,
  HTS_CODE AS HTS_CODE,
  HTS_DESC AS HTS_DESC,
  IMPORT_FLAG AS IMPORT_FLAG,
  MFGREP_NAME AS MFGREP_NAME,
  OWNBRAND_FLAG AS OWNBRAND_FLAG,
  PRIMARY_UPC AS PRIMARY_UPC,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
  SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  CONTENTS AS CONTENTS,
  CONTENTS_UNITS AS CONTENTS_UNITS,
  SKU_DESC AS SKU_DESC,
  SKU_NBR AS SKU_NBR,
  SKU_NBR_REF AS SKU_NBR_REF,
  STATELINE_FLAG AS STATELINE_FLAG,
  SUM_COST_NTL AS SUM_COST_NTL,
  SUM_RETAIL_NTL AS SUM_RETAIL_NTL,
  TUM_QTY AS TUM_QTY,
  VENDOR_SUB_RANGE AS VENDOR_SUB_RANGE,
  VOLUME AS VOLUME,
  VOLUME_UNITS AS VOLUME_UNITS,
  WEIGHT_GROSS AS WEIGHT_GROSS,
  WEIGHT_NET AS WEIGHT_NET,
  WEIGHT_UNITS AS WEIGHT_UNITS,
  WIDTH AS WIDTH,
  CATEGORY_DESC AS CATEGORY_DESC,
  CATEGORY_ID AS CATEGORY_ID,
  VENDOR_STYLE_NBR AS VENDOR_STYLE_NBR,
  PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
  COLOR AS COLOR,
  FLAVOR2 AS FLAVOR2,
  POND_FLAG AS POND_FLAG,
  PRODUCT_SIZE AS PRODUCT_SIZE,
  DATE_INIT_MKDN AS DATE_INIT_MKDN,
  ARTICLE_TYPE AS ARTICLE_TYPE,
  SAP_CLASS_DESC AS SAP_CLASS_DESC,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
  SEASON_DESC AS SEASON_DESC,
  SEASON_ID AS SEASON_ID,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  PLAN_GROUP_DESC AS PLAN_GROUP_DESC,
  PLAN_GROUP_ID AS PLAN_GROUP_ID,
  STATUS_ID AS STATUS_ID,
  STATUS_NAME AS STATUS_NAME,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  TAX_CLASS_ID AS TAX_CLASS_ID
FROM
  PRODUCT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("PRODUCT_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_PRODUCT_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  PLAN_GROUP_ID AS PLAN_GROUP_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  PRODUCT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_PRODUCT_1")

# COMMAND ----------
# DBTITLE 1, EXP_PRODUCT_2


query_2 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  PLAN_GROUP_ID AS PLAN_GROUP_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_PRODUCT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_PRODUCT_2")

# COMMAND ----------
# DBTITLE 1, PRODUCT_OLD


spark.sql("""INSERT INTO
  PRODUCT_OLD
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  PLAN_GROUP_ID AS PLAN_GROUP_ID
FROM
  EXP_PRODUCT_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_old")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_product_old", mainWorkflowId, parentName)
