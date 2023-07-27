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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_VENDOR")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_VENDOR", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_0


query_0 = f"""SELECT
  VendorNbr AS VendorNbr,
  VendorAccountGroup AS VendorAccountGroup,
  VendorName AS VendorName,
  CountryCD AS CountryCD,
  StreetAddress AS StreetAddress,
  AddressLine1 AS AddressLine1,
  AddressLine2 AS AddressLine2,
  City AS City,
  State AS State,
  Zip AS Zip,
  PhoneNbr1 AS PhoneNbr1,
  PhoneNbr2 AS PhoneNbr2,
  FaxNbr AS FaxNbr,
  POBoxNbr AS POBoxNbr,
  POBoxPostalCd AS POBoxPostalCd,
  LanguageCd AS LanguageCd,
  IndustryCd AS IndustryCd,
  TimeZone AS TimeZone,
  Region AS Region,
  CentralDeletionFlag AS CentralDeletionFlag,
  CentralPostingBlock AS CentralPostingBlock,
  CentralPurchasingBlock AS CentralPurchasingBlock,
  TaxNbr1 AS TaxNbr1,
  TaxNbr2 AS TaxNbr2,
  CustomerReferenceNbr AS CustomerReferenceNbr,
  CompanyIDforTradingPartner AS CompanyIDforTradingPartner,
  PayByEDIFlag AS PayByEDIFlag,
  ReturnToVendorEligibilityFlag AS ReturnToVendorEligibilityFlag,
  AlternatePayeeVendorCd AS AlternatePayeeVendorCd,
  VIPCD AS VIPCD,
  InactiveFlag AS InactiveFlag,
  IsVendorSubRangeRelevant AS IsVendorSubRangeRelevant,
  IsSiteLevelRelevant AS IsSiteLevelRelevant,
  IsOneTimeVendor AS IsOneTimeVendor,
  IsAlternatePayeeAllowed AS IsAlternatePayeeAllowed,
  StandardCarrierAccessCd AS StandardCarrierAccessCd,
  BlueGreenFlag AS BlueGreenFlag,
  IDOAFlag AS IDOAFlag,
  CarrierConfirmationExpectedFlag AS CarrierConfirmationExpectedFlag,
  TransportationChainCd AS TransportationChainCd,
  TransitTimeDays AS TransitTimeDays,
  TMSTransportationModeID AS TMSTransportationModeID,
  SCACCd AS SCACCd,
  PerformanceRatingID AS PerformanceRatingID,
  TenderMethodID AS TenderMethodID,
  CarrierContactName AS CarrierContactName,
  CarrierContactEmailAddress AS CarrierContactEmailAddress,
  CarrierGroupName AS CarrierGroupName,
  ShipmentStatusRequiredFlag AS ShipmentStatusRequiredFlag,
  ApplyFixedCostAllFlag AS ApplyFixedCostAllFlag,
  PickupInServiceFlag AS PickupInServiceFlag,
  PickupCutoffFlag AS PickupCutoffFlag,
  ResponseTime AS ResponseTime,
  ABSMinimumCharge AS ABSMinimumCharge,
  CMMaxHours AS CMMaxHours,
  CMFreeMilesi AS CMFreeMilesi,
  CMCapableFlag AS CMCapableFlag,
  CMDiscount AS CMDiscount,
  CMDiscountFirstLegFlag AS CMDiscountFirstLegFlag,
  CMVariableRate AS CMVariableRate,
  PackageDiscount AS PackageDiscount,
  Notes AS Notes,
  VendorAddDT AS VendorAddDT,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  Vendor"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR1_1


query_1 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  DATE_VEND_ADDED AS DATE_VEND_ADDED,
  DATE_VEND_REFRESHED AS DATE_VEND_REFRESHED,
  EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
  VENDOR_CTRY_ABBR AS VENDOR_CTRY_ABBR,
  VENDOR_CTRY AS VENDOR_CTRY,
  VENDOR_NAME AS VENDOR_NAME,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
  INCOTERM AS INCOTERM,
  CASH_TERM AS CASH_TERM,
  PAYMENT_TERM AS PAYMENT_TERM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR1_1")

# COMMAND ----------
# DBTITLE 1, VENDOR


spark.sql("""INSERT INTO
  VENDOR
SELECT
  VENDOR_ID AS VENDOR_ID,
  DATE_VEND_ADDED AS DATE_VEND_ADDED,
  DATE_VEND_REFRESHED AS DATE_VEND_REFRESHED,
  EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
  VENDOR_CTRY_ABBR AS VENDOR_CTRY_ABBR,
  VENDOR_CTRY AS VENDOR_CTRY,
  VENDOR_NAME AS VENDOR_NAME,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
  INCOTERM AS INCOTERM,
  CASH_TERM AS CASH_TERM,
  PAYMENT_TERM AS PAYMENT_TERM
FROM
  SQ_Shortcut_to_VENDOR1_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_VENDOR")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_VENDOR", mainWorkflowId, parentName)
