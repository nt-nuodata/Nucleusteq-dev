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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_primary_vendor_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_primary_vendor_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_VENDOR_DAY_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  DELETE_IND AS DELETE_IND,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  DELIV_EFF_DT AS DELIV_EFF_DT,
  DELIV_END_DT AS DELIV_END_DT,
  REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  ROUNDING_PROFILE_CD AS ROUNDING_PROFILE_CD,
  COUNTRY_CD AS COUNTRY_CD,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  LOAD_DT AS LOAD_DT
FROM
  SKU_VENDOR_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_VENDOR_DAY_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PROFILE_1


query_1 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  LOCATION_ID AS LOCATION_ID,
  SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
  PURCHASE_BLOCK AS PURCHASE_BLOCK,
  POSTING_BLOCK AS POSTING_BLOCK,
  DELETION_FLAG AS DELETION_FLAG,
  VIP_CD AS VIP_CD,
  INACTIVE_FLAG AS INACTIVE_FLAG,
  PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
  INCO_TERM_CD AS INCO_TERM_CD,
  ADDRESS AS ADDRESS,
  CITY AS CITY,
  STATE AS STATE,
  COUNTRY_CD AS COUNTRY_CD,
  ZIP AS ZIP,
  CONTACT AS CONTACT,
  CONTACT_PHONE AS CONTACT_PHONE,
  PHONE AS PHONE,
  PHONE_EXT AS PHONE_EXT,
  FAX AS FAX,
  RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  RTV_TYPE_CD AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD AS INDUSTRY_CD,
  LATITUDE AS LATITUDE,
  LONGITUDE AS LONGITUDE,
  TIME_ZONE_ID AS TIME_ZONE_ID,
  ADD_DT AS ADD_DT,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  VENDOR_PROFILE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_VENDOR_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, ASQ_SKU_VENDOR_DAY_2


query_2 = f"""SELECT
  /*+ INDEX (N, SKU_VENDOR_DAY_PK_IDX) */
  SVD.SKU_NBR AS SKU_NBR,
  SVD.VENDOR_ID AS VENDOR_ID,
  SVD.UNIT_NUMERATOR AS UNIT_NUMERATOR,
  SVD.UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  SVD.REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  SVD.DELIV_EFF_DT AS DELIV_EFF_DT,
  NVL(V.PARENT_VENDOR_ID, SVD.VENDOR_ID) AS PARENT_VENDOR_ID,
  CURRENT_TIMESTAMP AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_VENDOR_DAY_0 SVD
  LEFT OUTER JOIN Shortcut_to_VENDOR_PROFILE_1 V ON SVD.VENDOR_ID = V.VENDOR_ID
WHERE
  SVD.VENDOR_ID < 90000
  AND SVD.REGULAR_VENDOR_CD = 'X'
  AND SVD.DELETE_IND = ' '
ORDER BY
  SVD.SKU_NBR,
  SVD.REGULAR_VENDOR_CD,
  SVD.DELIV_EFF_DT,
  SVD.VENDOR_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_SKU_VENDOR_DAY_2")

# COMMAND ----------
# DBTITLE 1, AGGTRANS_3


query_3 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  last(VENDOR_ID) AS o_VENDOR_ID,
  last(UNIT_NUMERATOR) AS o_UNIT_NUMERATOR,
  last(UNIT_DENOMINATOR) AS o_UNIT_DENOMINATOR,
  LAST(REGULAR_VENDOR_CD) AS o_REGULAR_VENDOR_CD,
  last(PARENT_VENDOR_ID) AS o_PARENT_VENDOR_ID,
  LOAD_DT AS LOAD_DT,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  ASQ_SKU_VENDOR_DAY_2
GROUP BY
  SKU_NBR"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("AGGTRANS_3")

# COMMAND ----------
# DBTITLE 1, PRIMARY_VENDOR_PRE


spark.sql("""INSERT INTO
  PRIMARY_VENDOR_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  o_VENDOR_ID AS VENDOR_ID,
  o_UNIT_NUMERATOR AS UNIT_NUMERATOR,
  o_UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  o_REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  o_PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  LOAD_DT AS LOAD_DT
FROM
  AGGTRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_primary_vendor_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_primary_vendor_pre", mainWorkflowId, parentName)
