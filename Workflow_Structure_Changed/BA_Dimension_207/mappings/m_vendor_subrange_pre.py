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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_subrange_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_subrange_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SubRange1_0


query_0 = f"""SELECT
  SubRangeCD AS SubRangeCD,
  VendorNbr AS VendorNbr,
  SubRangeDesc AS SubRangeDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  SubRange"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SubRange1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SubRange_1


query_1 = f"""SELECT
  SubRangeCD AS SubRangeCD,
  VendorNbr AS VendorNbr,
  SubRangeDesc AS SubRangeDesc,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SubRange1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SubRange_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Vendor_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_Vendor_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Vendor_3


query_3 = f"""SELECT
  VendorNbr AS VendorNbr,
  VendorAccountGroup AS VendorAccountGroup,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Vendor_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_Vendor_3")

# COMMAND ----------
# DBTITLE 1, JNR_Subrange_4


query_4 = f"""SELECT
  DETAIL.VendorNbr AS VendorNbr,
  DETAIL.VendorAccountGroup AS VendorAccountGroup,
  MASTER.SubRangeCD AS SubRangeCD,
  MASTER.VendorNbr AS VendorNbr1,
  MASTER.SubRangeDesc AS SubRangeDesc,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SubRange_1 MASTER
  RIGHT JOIN SQ_Shortcut_to_Vendor_3 DETAIL ON MASTER.VendorNbr = DETAIL.VendorNbr"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_Subrange_4")

# COMMAND ----------
# DBTITLE 1, EXP_DEFAULT_5


query_5 = f"""SELECT
  SubRangeCD AS in_SubRangeCD,
  IFF(ISNULL (SubRangeCD), ' ', SubRangeCD) AS SubRangeCD,
  SubRangeDesc AS in_SubRangeDesc,
  IFF(ISNULL (SubRangeDesc), ' ', SubRangeDesc) AS SubRangeDesc,
  LTRIM(VendorNbr, '0') AS VendorNbr,
  VendorAccountGroup AS VendorAccountGroup,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Subrange_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_DEFAULT_5")

# COMMAND ----------
# DBTITLE 1, LKP_Vendor_Type_6


query_6 = f"""SELECT
  VT.VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  ED5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DEFAULT_5 ED5
  LEFT JOIN VENDOR_TYPE VT ON VT.VENDOR_ACCOUNT_GROUP_CD = ED5.VendorAccountGroup"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("LKP_Vendor_Type_6")

# COMMAND ----------
# DBTITLE 1, EXP_Vendor_type_7


query_7 = f"""SELECT
  ED5.VendorNbr AS VendorNbr,
  IFF(VendorAccountGroup = ' ', 21, LVT6.VENDOR_TYPE_ID) AS VENDOR_TYPE_ID,
  ED5.SubRangeCD AS SubRangeCD,
  ED5.SubRangeDesc AS SubRangeDesc,
  ED5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DEFAULT_5 ED5
  INNER JOIN LKP_Vendor_Type_6 LVT6 ON ED5.Monotonically_Increasing_Id = LVT6.Monotonically_Increasing_Id"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_Vendor_type_7")

# COMMAND ----------
# DBTITLE 1, VENDOR_SUBRANGE_PRE


spark.sql("""INSERT INTO
  VENDOR_SUBRANGE_PRE
SELECT
  VendorNbr AS VENDOR_NBR,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  SubRangeCD AS VENDOR_SUBRANGE_CD,
  SubRangeDesc AS VENDOR_SUBRANGE_DESC
FROM
  EXP_Vendor_type_7""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_subrange_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_subrange_pre", mainWorkflowId, parentName)
