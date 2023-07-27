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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_attribute_delete")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_attribute_delete", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SAP_ATTRIBUTE_PRE_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  DELETE_FLAG AS DELETE_FLAG
FROM
  SAP_ATTRIBUTE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SAP_ATTRIBUTE_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_PROFILE_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  SKU_TYPE AS SKU_TYPE,
  PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
  STATUS_ID AS STATUS_ID,
  SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
  SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
  SKU_DESC AS SKU_DESC,
  ALT_DESC AS ALT_DESC,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  COUNTRY_CD AS COUNTRY_CD,
  IMPORT_FLAG AS IMPORT_FLAG,
  HTS_CODE_ID AS HTS_CODE_ID,
  CONTENTS AS CONTENTS,
  CONTENTS_UNITS AS CONTENTS_UNITS,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  SIZE_DESC AS SIZE_DESC,
  BUM_QTY AS BUM_QTY,
  UOM_CD AS UOM_CD,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  BUYER_ID AS BUYER_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
  TAX_CLASS_ID AS TAX_CLASS_ID,
  VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
  BRAND_CD AS BRAND_CD,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  OWNBRAND_FLAG AS OWNBRAND_FLAG,
  STATELINE_FLAG AS STATELINE_FLAG,
  SIGN_TYPE_CD AS SIGN_TYPE_CD,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  INIT_MKDN_DT AS INIT_MKDN_DT,
  DISC_START_DT AS DISC_START_DT,
  ADD_DT AS ADD_DT,
  DELETE_DT AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  FIRST_SALE_DT AS FIRST_SALE_DT,
  LAST_SALE_DT AS LAST_SALE_DT,
  FIRST_INV_DT AS FIRST_INV_DT,
  LAST_INV_DT AS LAST_INV_DT,
  LOAD_DT AS LOAD_DT,
  BASE_NBR AS BASE_NBR,
  BP_COLOR_ID AS BP_COLOR_ID,
  BP_SIZE_ID AS BP_SIZE_ID,
  BP_BREED_ID AS BP_BREED_ID,
  BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
  BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
  BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
  CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
  NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
  NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
  RTV_DEPT_CD AS RTV_DEPT_CD,
  GL_ACCT_NBR AS GL_ACCT_NBR,
  ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
  COMPONENT_FLAG AS COMPONENT_FLAG,
  ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
  ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
  ZDISCO_PID_DT AS ZDISCO_PID_DT,
  ZDISCO_START_DT AS ZDISCO_START_DT,
  ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
  ZDISCO_DC_DT AS ZDISCO_DC_DT,
  ZDISCO_STR_DT AS ZDISCO_STR_DT,
  ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
  ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT
FROM
  SKU_PROFILE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_SKU_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SAP_ATTRIBUTE_PRE_2


query_2 = f"""SELECT
  S.PRODUCT_ID AS PRODUCT_ID,
  P.SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  P.SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  P.SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  P.DELETE_FLAG AS DELETE_FLAG,
  CURRENT_DATE AS UPDATE_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SAP_ATTRIBUTE_PRE_0 P,
  Shortcut_To_SKU_PROFILE_1 S
WHERE
  P.SKU_NBR = S.SKU_NBR
  AND P.DELETE_FLAG = 'X'"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_to_SAP_ATTRIBUTE_PRE_2")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_DELETE_FLAG_3


query_3 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_SAP_ATTRIBUTE_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_UPDATE_DELETE_FLAG_3")

# COMMAND ----------
# DBTITLE 1, SAP_ATTRIBUTE


spark.sql("""MERGE INTO SAP_ATTRIBUTE AS TARGET
USING
  UPD_UPDATE_DELETE_FLAG_3 AS SOURCE ON TARGET.SAP_ATT_TYPE_ID = SOURCE.SAP_ATT_TYPE_ID
  AND TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  AND TARGET.SAP_ATT_CODE_ID = SOURCE.SAP_ATT_CODE_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.SAP_ATT_TYPE_ID = SOURCE.SAP_ATT_TYPE_ID,
  TARGET.SAP_ATT_CODE_ID = SOURCE.SAP_ATT_CODE_ID,
  TARGET.SAP_ATT_VALUE_ID = SOURCE.SAP_ATT_VALUE_ID,
  TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_attribute_delete")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_attribute_delete", mainWorkflowId, parentName)
