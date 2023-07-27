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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_from_sku_delta_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_uom_from_sku_delta_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_PROFILE_PRE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  DELETE_IND AS DELETE_IND,
  ALT_DESC AS ALT_DESC,
  BUM_QTY AS BUM_QTY,
  BUYER_ID AS BUYER_ID,
  BUYER_NAME AS BUYER_NAME,
  CONTENTS AS CONTENTS,
  CONTENTS_UNITS AS CONTENTS_UNITS,
  ADD_DT AS ADD_DT,
  DELETE_DT AS DELETE_DT,
  HTS_CODE_ID AS HTS_CODE_ID,
  HTS_CODE_DESC AS HTS_CODE_DESC,
  PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
  SKU_TYPE AS SKU_TYPE,
  SKU_DESC AS SKU_DESC,
  SKU_NBR AS SKU_NBR,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC,
  SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CLASS_DESC AS SAP_CLASS_DESC,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  DISC_START_DT AS DISC_START_DT,
  BRAND_NAME AS BRAND_NAME,
  IMPORT_FLAG AS IMPORT_FLAG,
  STATELINE_FLAG AS STATELINE_FLAG,
  SIZE_DESC AS SIZE_DESC,
  INIT_MKDN_DT AS INIT_MKDN_DT,
  STATUS_ID AS STATUS_ID,
  STATUS_NAME AS STATUS_NAME,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  TAX_CLASS_ID AS TAX_CLASS_ID,
  TAX_CLASS_DESC AS TAX_CLASS_DESC,
  VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
  BASE_UOM_CD AS BASE_UOM_CD,
  SIGN_TYPE_CD AS SIGN_TYPE_CD,
  OWNBRAND_FLAG AS OWNBRAND_FLAG,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_NAME AS COUNTRY_NAME,
  CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
  RTV_DEPT_CD AS RTV_DEPT_CD
FROM
  SKU_PROFILE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UOM_STANDARD_1


query_1 = f"""SELECT
  UOM_STD_CD AS UOM_STD_CD,
  DIMENSION AS DIMENSION,
  NUMERATOR AS NUMERATOR,
  DENOMINATOR AS DENOMINATOR,
  EXPONENT AS EXPONENT,
  ADD_CONSTANT AS ADD_CONSTANT,
  DEC_PLACES AS DEC_PLACES,
  ISO_CODE AS ISO_CODE
FROM
  UOM_STANDARD"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_UOM_STANDARD_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_UOM_2


query_2 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  UOM_CD AS UOM_CD,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UOM_CD AS VOLUME_UOM_CD,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  SCM_VOLUME_UOM_CD AS SCM_VOLUME_UOM_CD,
  SCM_VOLUME_AMT AS SCM_VOLUME_AMT,
  SCM_WEIGHT_UOM_CD AS SCM_WEIGHT_UOM_CD,
  SCM_WEIGHT_NET_AMT AS SCM_WEIGHT_NET_AMT,
  DELETE_DT AS DELETE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SKU_UOM"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_SKU_UOM_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_UOM_3


query_3 = f"""SELECT
  S.PRODUCT_ID AS PRODUCT_ID,
  S.UOM_CD AS UOM_CD,
  T.UOM_WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  CURRENT_DATE AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_UOM_2 S,
  (
    SELECT
      K.PRODUCT_ID,
      S.UOM_CD,
      S.UOM_NUMERATOR,
      S.UOM_DENOMINATOR,
      TRIM(S.WEIGHT_UOM_CD) AS WEIGHT_UOM_CD,
      TRIM(K.WEIGHT_UNIT_DESC) AS PROF_WEIGHT_UOM_CD,
      K.WEIGHT_NET_AMT AS PROF_WEIGHT_NET_AMT,
      U1.NUMERATOR AS UOM_WGT_NUMERATOR,
      U1.DENOMINATOR AS UOM_WGT_DENOMINATOR,
      U2.NUMERATOR AS PROF_WGT_NUMERATOR,
      U2.DENOMINATOR AS PROF_WGT_DENOMINATOR,
      ROUND(
        K.WEIGHT_NET_AMT * (
          TO_NUMBER(S.UOM_NUMERATOR, '999999') / TO_NUMBER(S.UOM_DENOMINATOR, '999')
        ) * (
          TO_NUMBER(U2.NUMERATOR, '9999999999') / TO_NUMBER(U2.DENOMINATOR, '9999999999')
        ) * (
          TO_NUMBER(U1.DENOMINATOR, '9999999999') / TO_NUMBER(U1.NUMERATOR, '9999999999')
        ),
        3
      ) AS UOM_WEIGHT_NET_AMT
    FROM
      SKU_PROFILE_PRE K,
      Shortcut_To_SKU_UOM_2 S,
      UOM_STANDARD U1,
      UOM_STANDARD U2
    WHERE
      K.PRODUCT_ID = S.PRODUCT_ID
      AND TRIM(S.WEIGHT_UOM_CD) = TRIM(U1.UOM_STD_CD)
      AND TRIM(K.WEIGHT_UNIT_DESC) = TRIM(U2.UOM_STD_CD)
  ) T
WHERE
  S.PRODUCT_ID = T.PRODUCT_ID
  AND S.UOM_CD = T.UOM_CD
  AND S.WEIGHT_NET_AMT <> T.UOM_WEIGHT_NET_AMT"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_To_SKU_UOM_3")

# COMMAND ----------
# DBTITLE 1, UPD_update_4


query_4 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  UOM_CD AS UOM_CD,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  LOAD_DT AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_UOM_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("UPD_update_4")

# COMMAND ----------
# DBTITLE 1, SKU_UOM


spark.sql("""MERGE INTO SKU_UOM AS TARGET
USING
  UPD_update_4 AS SOURCE ON TARGET.UOM_CD = SOURCE.UOM_CD
  AND TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.UOM_CD = SOURCE.UOM_CD,
  TARGET.WEIGHT_NET_AMT = SOURCE.WEIGHT_NET_AMT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_from_sku_delta_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_uom_from_sku_delta_UPD", mainWorkflowId, parentName)
