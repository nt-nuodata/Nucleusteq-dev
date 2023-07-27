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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_vendor_profile")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_vendor_profile", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PROFILE_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR_PROFILE_1


query_1 = f"""SELECT
  VENDOR_ID,
  VENDOR_NAME,
  VENDOR_TYPE_ID,
  VENDOR_NBR,
  LOCATION_ID,
  PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME,
  EDI_ELIG_FLAG,
  PAYMENT_TERM_CD,
  INCO_TERM_CD,
  ADDRESS,
  CITY,
  STATE,
  COUNTRY_CD,
  ZIP,
  CONTACT,
  CONTACT_PHONE,
  PHONE,
  PHONE_EXT,
  FAX,
  RTV_ELIG_FLAG,
  RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD,
  LATITUDE,
  LONGITUDE,
  TIME_ZONE_ID,
  ADD_DT,
  UPDATE_DT,
  LOAD_DT
FROM
  Shortcut_to_VENDOR_PROFILE_0
WHERE
  UPDATE_DT >= CURRENT_DATE - 1
  OR LOAD_DT >= CURRENT_DATE - 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE


spark.sql("""INSERT INTO
  VENDOR_PROFILE
SELECT
  VENDOR_ID AS VENDOR_ID,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  LOCATION_ID AS LOCATION_ID,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
  EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
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
  SQ_Shortcut_to_VENDOR_PROFILE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_vendor_profile")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_vendor_profile", mainWorkflowId, parentName)
