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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile_superiorvendor")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_profile_superiorvendor", variablesTableName, mainWorkflowId)

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
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_PROFILE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, LKP_VENDOR_PROFILE_PRE_2


query_2 = f"""SELECT
  VPP.SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
  SStVP1.VENDOR_TYPE_ID AS VENDOR_TYPE_ID1,
  SStVP1.VENDOR_NBR AS VENDOR_NBR1,
  SStVP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_VENDOR_PROFILE_1 SStVP1
  LEFT JOIN VENDOR_PROFILE_PRE VPP ON VPP.VENDOR_TYPE_ID = SStVP1.VENDOR_TYPE_ID
  AND VPP.VENDOR_NBR = SStVP1.VENDOR_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_VENDOR_PROFILE_PRE_2")

# COMMAND ----------
# DBTITLE 1, EXP_3


query_3 = f"""SELECT
  SStVP1.VENDOR_ID AS VENDOR_ID,
  SStVP1.VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  LVPP2.SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
  SStVP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_VENDOR_PROFILE_1 SStVP1
  INNER JOIN LKP_VENDOR_PROFILE_PRE_2 LVPP2 ON SStVP1.Monotonically_Increasing_Id = LVPP2.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_3")

# COMMAND ----------
# DBTITLE 1, LKP_DM_PG_VENDOR_4


query_4 = f"""SELECT
  DPV.VENDOR_ID AS VENDOR_ID,
  DPV.PURCH_GROUP_ID AS PURCH_GROUP_ID,
  E3.VENDOR_ID AS VENDOR_ID1,
  E3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_3 E3
  LEFT JOIN DM_PG_VENDOR DPV ON DPV.VENDOR_ID = E3.VENDOR_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_DM_PG_VENDOR_4")

# COMMAND ----------
# DBTITLE 1, LKP_VENDOR_PROFILE1_5


query_5 = f"""SELECT
  VP.VENDOR_ID AS VENDOR_ID,
  VP.VENDOR_NAME AS VENDOR_NAME,
  E3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_3 E3
  LEFT JOIN VENDOR_PROFILE VP ON VP.VENDOR_TYPE_ID = E3.VENDOR_TYPE_ID
  AND VP.VENDOR_NBR = E3.SUPERIOR_VENDOR_NBR"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_VENDOR_PROFILE1_5")

# COMMAND ----------
# DBTITLE 1, UPD_Update_6


query_6 = f"""SELECT
  E3.VENDOR_ID AS VENDOR_ID,
  E3.SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_ID,
  LVP5.VENDOR_NAME AS VENDOR_NAME,
  LDPV4.PURCH_GROUP_ID AS PURCH_GROUP_ID,
  E3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_3 E3
  INNER JOIN LKP_DM_PG_VENDOR_4 LDPV4 ON E3.Monotonically_Increasing_Id = LDPV4.Monotonically_Increasing_Id
  INNER JOIN LKP_VENDOR_PROFILE1_5 LVP5 ON LDPV4.Monotonically_Increasing_Id = LVP5.Monotonically_Increasing_Id"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("UPD_Update_6")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE


spark.sql("""MERGE INTO VENDOR_PROFILE AS TARGET
USING
  UPD_Update_6 AS SOURCE ON TARGET.VENDOR_ID = SOURCE.VENDOR_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.VENDOR_ID = SOURCE.VENDOR_ID,
  TARGET.SUPERIOR_VENDOR_ID = SOURCE.SUPERIOR_VENDOR_ID,
  TARGET.PARENT_VENDOR_ID = SOURCE.SUPERIOR_VENDOR_ID,
  TARGET.PARENT_VENDOR_NAME = SOURCE.VENDOR_NAME,
  TARGET.PURCH_GROUP_ID = SOURCE.PURCH_GROUP_ID""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile_superiorvendor")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_profile_superiorvendor", mainWorkflowId, parentName)
