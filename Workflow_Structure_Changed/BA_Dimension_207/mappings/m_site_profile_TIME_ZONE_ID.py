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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_TIME_ZONE_ID")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_profile_TIME_ZONE_ID", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TimeZone_0


query_0 = f"""SELECT
  TimeZoneId AS TimeZoneId,
  TimeZoneDesc AS TimeZoneDesc,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp,
  TimeZoneTlmsId AS TimeZoneTlmsId,
  TimeZoneTlmsName AS TimeZoneTlmsName
FROM
  TimeZone"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_TimeZone_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PetSmartFacility_1


query_1 = f"""SELECT
  FacilityGid AS FacilityGid,
  FacilityTypeId AS FacilityTypeId,
  FacilityNbr AS FacilityNbr,
  FacilityName AS FacilityName,
  FacilityDesc AS FacilityDesc,
  CompanyCode AS CompanyCode,
  TimeZoneId AS TimeZoneId,
  SrcCreateTstmp AS SrcCreateTstmp,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp
FROM
  PetSmartFacility"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PetSmartFacility_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_FacilityEDWXref_2


query_2 = f"""SELECT
  FacilityGid AS FacilityGid,
  LocationID AS LocationID,
  LocationNbr AS LocationNbr,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp
FROM
  FacilityEDWXref"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_FacilityEDWXref_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PetSmartFacility_3


query_3 = f"""SELECT
  Shortcut_to_FacilityEDWXref_2.LocationID AS LocationID,
  Shortcut_to_TimeZone_0.TimeZoneDesc AS TimeZoneDesc,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PetSmartFacility_3")

# COMMAND ----------
# DBTITLE 1, EXP_SITE_PROFILE_4


query_4 = f"""SELECT
  LocationID AS LocationID,
  TimeZoneDesc AS TimeZoneDesc,
  sysdate AS Update_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PetSmartFacility_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_SITE_PROFILE_4")

# COMMAND ----------
# DBTITLE 1, UPD_SITE_PROFILE_5


query_5 = f"""SELECT
  LocationID AS LocationID,
  TimeZoneDesc AS TimeZoneDesc,
  Update_DT AS Update_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_SITE_PROFILE_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("UPD_SITE_PROFILE_5")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE


spark.sql("""MERGE INTO SITE_PROFILE AS TARGET
USING
  UPD_SITE_PROFILE_5 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LocationID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LocationID,
  TARGET.TIME_ZONE_ID = SOURCE.TimeZoneDesc,
  TARGET.UPDATE_DT = SOURCE.Update_DT""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_TIME_ZONE_ID")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_profile_TIME_ZONE_ID", mainWorkflowId, parentName)
