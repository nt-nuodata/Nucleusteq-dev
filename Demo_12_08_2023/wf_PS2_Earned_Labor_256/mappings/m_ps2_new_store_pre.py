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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_new_store_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_new_store_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_vw_PetsMartFacility_0


query_0 = f"""SELECT
  FacilityGid AS FacilityGid,
  FacilityNbr AS FacilityNbr,
  EDWLocationId AS EDWLocationId,
  LocationNbr AS LocationNbr,
  FacilityName AS FacilityName,
  FacilityDesc AS FacilityDesc,
  FacilityTypeId AS FacilityTypeId,
  FacilityTypeDesc AS FacilityTypeDesc,
  CompanyCode AS CompanyCode,
  PhoneNbr AS PhoneNbr,
  FaxNbr AS FaxNbr,
  EmailAddress AS EmailAddress,
  StatusFlag AS StatusFlag,
  TimeZoneId AS TimeZoneId,
  TimeZoneDesc AS TimeZoneDesc,
  AddrLine1 AS AddrLine1,
  AddrLine2 AS AddrLine2,
  City AS City,
  StateProvCd AS StateProvCd,
  CountryCd AS CountryCd,
  PostalCd AS PostalCd,
  Zip5 AS Zip5,
  Zip4 AS Zip4,
  County AS County,
  Latitude AS Latitude,
  Longitude AS Longitude,
  RegionId AS RegionId,
  DistrictId AS DistrictId,
  HotelFlag AS HotelFlag,
  GroomFlag AS GroomFlag,
  DaycampFlag AS DaycampFlag,
  TrainingFlag AS TrainingFlag,
  AdoptionFlag AS AdoptionFlag,
  VetFlag AS VetFlag,
  OpenDate AS OpenDate,
  CloseDate AS CloseDate,
  SoftOpenDate AS SoftOpenDate,
  GrandOpenDate AS GrandOpenDate,
  JurisdictionTax AS JurisdictionTax,
  CityTaxRate AS CityTaxRate,
  CountyTaxRate AS CountyTaxRate,
  StateTaxRate AS StateTaxRate,
  PSTRate AS PSTRate,
  GSTRate AS GSTRate,
  PrimaryDCNbr AS PrimaryDCNbr,
  ForwardDCNbr AS ForwardDCNbr,
  FishDCNbr AS FishDCNbr,
  DeliverySvcClassId AS DeliverySvcClassId,
  PickSvcClassId AS PickSvcClassId,
  TradeArea AS TradeArea,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp
FROM
  vw_PetsMartFacility"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_vw_PetsMartFacility_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_vw_PetsMartFacility_1


query_1 = f"""SELECT
  FacilityNbr AS FacilityNbr,
  SoftOpenDate AS SoftOpenDate,
  GrandOpenDate AS GrandOpenDate,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_vw_PetsMartFacility_0
WHERE
  Shortcut_to_vw_PetsMartFacility_0.SoftOpenDate BETWEEN GETDATE() - DATEPART(WEEKDAY, GETDATE()) - 41
  AND GETDATE() - DATEPART(WEEKDAY, GETDATE())"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_vw_PetsMartFacility_1")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_2


query_2 = f"""SELECT
  SP.LOCATION_ID AS LOCATION_ID,
  SP.STORE_NBR AS STORE_NBR,
  SStvP1.FacilityNbr AS FacilityNbr,
  SStvP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_vw_PetsMartFacility_1 SStvP1
  LEFT JOIN SITE_PROFILE SP ON SP.STORE_NBR = SStvP1.FacilityNbr"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, EXP_GRAND_OPENING_HRS_AND_WEEK_DT_3


query_3 = f"""SELECT
  ADD_TO_DATE(now(), 'DAY', 1 - TO_INTEGER(TO_CHAR(now(), 'D'))) AS WEEK_DT,
  LSP2.LOCATION_ID AS LOCATION_ID,
  SStvP1.FacilityNbr AS FacilityNbr,
  SStvP1.SoftOpenDate AS SoftOpenDate,
  SStvP1.GrandOpenDate AS in_GrandOpenDate,
  IFF(
    SStvP1.GrandOpenDate > ADD_TO_DATE(now(), 'DD', -8),
    175,
    0
  ) AS GRAND_OPENING_HRS,
  SStvP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_vw_PetsMartFacility_1 SStvP1
  INNER JOIN LKP_SITE_PROFILE_2 LSP2 ON SStvP1.Monotonically_Increasing_Id = LSP2.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_GRAND_OPENING_HRS_AND_WEEK_DT_3")

# COMMAND ----------
# DBTITLE 1, PS2_NEW_STORE_PRE


spark.sql("""INSERT INTO
  PS2_NEW_STORE_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  FacilityNbr AS STORE_NBR,
  SoftOpenDate AS SFT_OPEN_DT,
  NULL AS PLAN_SALES_AMT,
  NULL AS ACTUAL_SALES_AMT,
  NULL AS CUSTOM_AD_HOC_HRS,
  GRAND_OPENING_HRS AS GRAND_OPENING_HRS
FROM
  EXP_GRAND_OPENING_HRS_AND_WEEK_DT_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_new_store_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_new_store_pre", mainWorkflowId, parentName)
