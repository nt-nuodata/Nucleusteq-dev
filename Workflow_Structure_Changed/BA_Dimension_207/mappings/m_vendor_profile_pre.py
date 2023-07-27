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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_profile_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Vendor_0


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

df_0.createOrReplaceTempView("Shortcut_to_Vendor_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Vendor_1


query_1 = f"""SELECT
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
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Vendor_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Vendor_1")

# COMMAND ----------
# DBTITLE 1, LKP_Company_vendor_Relation_2


query_2 = f"""SELECT
  CVR.VendorNbr AS VendorNbr,
  CVR.EDIEligibilityFlag AS EDIEligibilityFlag,
  SStV1.VendorNbr AS VendorNbr1,
  SStV1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Vendor_1 SStV1
  LEFT JOIN (
    SELECT
      max(Company_Vendor_Relation.EDIEligibilityFlag) as EDIEligibilityFlag,
      Company_Vendor_Relation.VendorNbr as VendorNbr
    FROM
      Company_Vendor_Relation
    group by
      Company_Vendor_Relation.VendorNbr
  ) AS CVR ON CVR.VendorNbr = SStV1.VendorNbr"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_Company_vendor_Relation_2")

# COMMAND ----------
# DBTITLE 1, LKP_LFA1_PRE_3


query_3 = f"""SELECT
  LP.LIFNR AS LIFNR,
  LP.WERKS AS WERKS,
  SStV1.VendorNbr AS VendorNbr,
  SStV1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Vendor_1 SStV1
  LEFT JOIN (
    SELECT
      LFA1_PRE.WERKS as WERKS,
      TRIM(LFA1_PRE.LIFNR) as LIFNR
    FROM
      LFA1_PRE --) AS LP ON LP.LIFNR = SStV1.VendorNbr"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("LKP_LFA1_PRE_3")

# COMMAND ----------
# DBTITLE 1, EXP_VENDOR1_4


query_4 = f"""SELECT
  VendorNbr AS VendorNbr,
  VendorAccountGroup AS VendorAccountGroup,
  VendorName AS VendorName,
  CountryCD AS CountryCD,
  StreetAddress AS StreetAddress,
  City AS City,
  State AS State,
  Zip AS Zip,
  PhoneNbr1 AS PhoneNbr1,
  IndustryCd AS IndustryCd,
  Region AS Region,
  CentralDeletionFlag AS CentralDeletionFlag,
  CentralPostingBlock AS CentralPostingBlock,
  CentralPurchasingBlock AS CentralPurchasingBlock,
  FaxNbr AS FaxNbr,
  InactiveFlag AS InactiveFlag,
  VendorAddDT AS VendorAddDT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Vendor_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_VENDOR1_4")

# COMMAND ----------
# DBTITLE 1, LKP_Purchasing_Organization_to_vendor_5


query_5 = f"""SELECT
  PtV.VendorNbr AS VendorNbr,
  PtV.PurchasingOrganizationCd AS PurchasingOrganizationCd,
  PtV.PurchasingGroupId AS PurchasingGroupId,
  PtV.SuperiorVendorNbr AS SuperiorVendorNbr,
  PtV.SalesPersonName AS SalesPersonName,
  PtV.PhoneNbr AS PhoneNbr,
  PtV.OrderCurrency AS OrderCurrency,
  PtV.LeadTime AS LeadTime,
  PtV.IncoTermCd AS IncoTermCd,
  PtV.PaymentTermCd AS PaymentTermCd,
  PtV.USRTVTypeCd AS USRTVTypeCd,
  PtV.CARTVTypeCd AS CARTVTypeCd,
  PtV.RTVFreightTypeCd AS RTVFreightTypeCd,
  PtV.RTVEligFlag AS RTVEligFlag,
  PtV.VIPCD AS VIPCD,
  PtV.WEBFlag AS WEBFlag,
  PtV.CashDiscFlag AS CashDiscFlag,
  PtV.BlockedFlag AS BlockedFlag,
  PtV.BlockedDate AS BlockedDate,
  PtV.LoadTstmp AS LoadTstmp,
  PtV.UpdateTstmp AS UpdateTstmp,
  EV4.VendorNbr AS VendorNbr1,
  EV4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_VENDOR1_4 EV4
  LEFT JOIN PurchasingOrganization_to_Vendor PtV ON PtV.VendorNbr = EV4.VendorNbr"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_Purchasing_Organization_to_vendor_5")

# COMMAND ----------
# DBTITLE 1, EXP_FINAL_6


query_6 = f"""SELECT
  EV4.VendorNbr AS VendorNbr,
  EV4.VendorAccountGroup AS i_VendorAccountGroup,
  IFF(
    EV4.VendorAccountGroup = '0012',
    1,
    IFF(
      EV4.VendorAccountGroup = 'ZMER',
      2,
      IFF(
        EV4.VendorAccountGroup = '0100',
        10,
        IFF(
          EV4.VendorAccountGroup = '0007',
          11,
          IFF(
            EV4.VendorAccountGroup = 'ZEXP',
            12,
            IFF(
              EV4.VendorAccountGroup = 'ZEMP',
              13,
              IFF(
                EV4.VendorAccountGroup = 'ZONE',
                14,
                IFF(
                  EV4.VendorAccountGroup = 'ZBNK',
                  15,
                  IFF(
                    EV4.VendorAccountGroup = '0002',
                    16,
                    IFF(
                      EV4.VendorAccountGroup = '0003',
                      17,
                      IFF(
                        EV4.VendorAccountGroup = '0006',
                        18,
                        IFF(
                          EV4.VendorAccountGroup = 'R007',
                          19,
                          IFF(EV4.VendorAccountGroup = 'R100', 20, 21)
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  ) AS VendorAccountGroup,
  EV4.VendorName AS VendorName,
  LLP3.WERKS AS SITE,
  LCvR2.EDIEligibilityFlag AS in_EDIEligibilityFlag,
  IFF(
    IN(EV4.VendorAccountGroup, 'ZMER', 'ZEXP') = 1,
    IFF(LCvR2.EDIEligibilityFlag = 'X', 'Y', 'N'),
    ' '
  ) AS EDIEligibilityFlag1,
  EV4.CentralDeletionFlag AS CentralDeletionFlag,
  EV4.CentralPostingBlock AS CentralPostingBlock,
  EV4.CentralPurchasingBlock AS CentralPurchasingBlock,
  LPOtv5.VIPCD AS VIPCD,
  EV4.InactiveFlag AS InactiveFlag,
  LPOtv5.IncoTermCd AS IncoTermCd,
  LPOtv5.PaymentTermCd AS PaymentTermCd,
  LPOtv5.RTVFreightTypeCd AS RTVFreightTypeCd,
  EV4.CountryCD AS CountryCD,
  EV4.StreetAddress AS StreetAddress,
  EV4.City AS City,
  EV4.State AS State,
  EV4.Zip AS Zip,
  EV4.FaxNbr AS FaxNbr,
  EV4.IndustryCd AS IndustryCd,
  LPOtv5.RTVEligFlag AS ReturnToVendorEligibilityFlag,
  EV4.VendorAddDT AS VendorAddDT,
  EV4.PhoneNbr1 AS PhoneNbr,
  LPOtv5.SalesPersonName AS Contact,
  LPOtv5.PhoneNbr1 AS Contact_Phone,
  LPOtv5.PurchasingGroupId AS PurchasingGroupId,
  LPOtv5.SuperiorVendorNbr AS SuperiorVendorNbr,
  LPOtv5.USRTVTypeCd AS USRTVTypeCd,
  LPOtv5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_Purchasing_Organization_to_vendor_5 LPOtv5
  INNER JOIN EXP_VENDOR1_4 EV4 ON LPOtv5.Monotonically_Increasing_Id = EV4.Monotonically_Increasing_Id
  INNER JOIN LKP_LFA1_PRE_3 LLP3 ON EV4.Monotonically_Increasing_Id = LLP3.Monotonically_Increasing_Id
  INNER JOIN LKP_Company_vendor_Relation_2 LCvR2 ON LLP3.Monotonically_Increasing_Id = LCvR2.Monotonically_Increasing_Id"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_FINAL_6")

# COMMAND ----------
# DBTITLE 1, FLTR_7


query_7 = f"""SELECT
  VendorNbr AS VendorNbr,
  VendorAccountGroup AS VendorAccountGroup,
  VendorName AS VendorName,
  SITE AS SITE,
  EDIEligibilityFlag1 AS EDIEligibilityFlag,
  CentralDeletionFlag AS CentralDeletionFlag,
  CentralPostingBlock AS CentralPostingBlock,
  CentralPurchasingBlock AS CentralPurchasingBlock,
  VIPCD AS VIPCD,
  InactiveFlag AS InactiveFlag,
  IncoTermCd AS IncoTermCd,
  PaymentTermCd AS PaymentTermCd,
  RTVFreightTypeCd AS RTVFreightTypeCd,
  CountryCD AS CountryCD,
  StreetAddress AS StreetAddress,
  City AS City,
  State AS State,
  Zip AS Zip,
  FaxNbr AS FaxNbr,
  IndustryCd AS IndustryCd,
  ReturnToVendorEligibilityFlag AS ReturnToVendorEligibilityFlag,
  VendorAddDT AS VendorAddDT,
  PhoneNbr AS PhoneNbr,
  Contact AS Contact,
  Contact_Phone AS Contact_Phone,
  PurchasingGroupId AS PurchasingGroupId,
  SuperiorVendorNbr AS SuperiorVendorNbr,
  USRTVTypeCd AS USRTVTypeCd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_FINAL_6
WHERE
  --not in(VendorNbr ,'0')
  1"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FLTR_7")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_PRE


spark.sql("""INSERT INTO
  VENDOR_PROFILE_PRE
SELECT
  VendorAccountGroup AS VENDOR_TYPE_ID,
  VendorNbr AS VENDOR_NBR,
  VendorName AS VENDOR_NAME,
  NULL AS VENDOR_GROUP,
  SITE AS SITE,
  NULL AS PARENT_VENDOR_ID,
  PurchasingGroupId AS PURCH_GROUP_ID,
  SuperiorVendorNbr AS SUPERIOR_VENDOR_NBR,
  EDIEligibilityFlag AS EDI_ELIG_FLAG,
  CentralPurchasingBlock AS PURCHASE_BLOCK,
  CentralPostingBlock AS POSTING_BLOCK,
  CentralDeletionFlag AS DELETION_FLAG,
  VIPCD AS VIP_CD,
  InactiveFlag AS INACTIVE_FLAG,
  PaymentTermCd AS PAYMENT_TERM_CD,
  IncoTermCd AS INCO_TERM_CD,
  StreetAddress AS ADDRESS,
  City AS CITY,
  State AS STATE,
  CountryCD AS COUNTRY_CD,
  Zip AS ZIP,
  Contact AS CONTACT,
  Contact_Phone AS CONTACT_PHONE,
  PhoneNbr AS PHONE,
  FaxNbr AS FAX,
  ReturnToVendorEligibilityFlag AS RTV_ELIG_FLAG,
  USRTVTypeCd AS RTV_TYPE_CD,
  RTVFreightTypeCd AS RTV_FREIGHT_TYPE_CD,
  IndustryCd AS INDUSTRY_CD,
  VendorAddDT AS ADD_DT
FROM
  FLTR_7""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_profile_pre", mainWorkflowId, parentName)
