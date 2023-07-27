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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_store_tax_rate")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_store_tax_rate", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STORE_TAX_RATE_FLAT_0


query_0 = f"""SELECT
  SITE_NBR AS SITE_NBR,
  JURISDICTION_TAX AS JURISDICTION_TAX,
  CITY_TAX AS CITY_TAX,
  COUNTY_TAX AS COUNTY_TAX,
  STATE_TAX AS STATE_TAX
FROM
  STORE_TAX_RATE_FLAT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STORE_TAX_RATE_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1


query_1 = f"""SELECT
  SITE_NBR AS SITE_NBR,
  JURISDICTION_TAX AS JURISDICTION_TAX,
  CITY_TAX AS CITY_TAX,
  COUNTY_TAX AS COUNTY_TAX,
  STATE_TAX AS STATE_TAX,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_STORE_TAX_RATE_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1")

# COMMAND ----------
# DBTITLE 1, LKP_PetsmartFacility_2


query_2 = f"""SELECT
  P.FacilityGid AS FacilityGid,
  SStSTRF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 SStSTRF1
  LEFT JOIN PetSmartFacility P ON P.FacilityNbr = SStSTRF1.SITE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_PetsmartFacility_2")

# COMMAND ----------
# DBTITLE 1, LKP_FacilityAddress_3


query_3 = f"""SELECT
  F.FacilityGid AS FacilityGid,
  F.CountryCd AS CountryCd,
  LP2.FacilityGid AS FacilityGid_PetsmartFacility,
  LP2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_PetsmartFacility_2 LP2
  LEFT JOIN FacilityAddress F ON F.FacilityGid = LP2.FacilityGid"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("LKP_FacilityAddress_3")

# COMMAND ----------
# DBTITLE 1, FLT_FacilityGID_Not_Null_4


query_4 = f"""SELECT
  LF3.FacilityGid AS FacilityGid,
  LF3.CountryCd AS COUNTRY_CD,
  SStSTRF1.JURISDICTION_TAX AS JURISDICTION_TAX,
  SStSTRF1.CITY_TAX AS CITY_TAX,
  SStSTRF1.COUNTY_TAX AS COUNTY_TAX,
  SStSTRF1.STATE_TAX AS STATE_TAX,
  SStSTRF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 SStSTRF1
  INNER JOIN LKP_FacilityAddress_3 LF3 ON SStSTRF1.Monotonically_Increasing_Id = LF3.Monotonically_Increasing_Id
WHERE
  NOT ISNULL(LF3.FacilityGid)"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("FLT_FacilityGID_Not_Null_4")

# COMMAND ----------
# DBTITLE 1, EXP_StoreRetail_5


query_5 = f"""SELECT
  FacilityGid AS FacilityGid,
  CITY_TAX AS CityTaxRate,
  IFF(COUNTRY_CD = 'CA', 0, COUNTY_TAX) AS CountyTaxRate,
  IFF(COUNTRY_CD = 'CA', 0, STATE_TAX) AS StateTaxRate,
  JURISDICTION_TAX AS JurisdictionTax,
  IFF(COUNTRY_CD = 'CA', COUNTY_TAX, 0) AS PSTRate,
  IFF(COUNTRY_CD = 'CA', STATE_TAX, 0) AS GSTRate,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FLT_FacilityGID_Not_Null_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_StoreRetail_5")

# COMMAND ----------
# DBTITLE 1, LKPTRANS_6


query_6 = f"""SELECT
  SStSTRF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 SStSTRF1
  LEFT JOIN (
    SELECT
      TRIM(SITE_PROFILE.COUNTRY_CD) AS COUNTRY_CD,
      SITE_PROFILE.STORE_NBR AS STORE_NBR
    FROM
      SITE_PROFILE
  ) AS SP ON SP.STORE_NBR = SStSTRF1.SITE_NBR"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("LKPTRANS_6")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_7


query_7 = f"""SELECT
  SStSTRF1.SITE_NBR AS SITE_NBR,
  L6.COUNTRY_CD AS COUNTRY_CD,
  SStSTRF1.JURISDICTION_TAX AS JURISDICTION_TAX,
  SStSTRF1.CITY_TAX AS CITY_TAX,
  IFF(L6.COUNTRY_CD = 'CA', 0, SStSTRF1.COUNTY_TAX) AS o_COUNTY_TAX,
  IFF(L6.COUNTRY_CD = 'CA', 0, SStSTRF1.STATE_TAX) AS o_STATE_TAX,
  IFF(L6.COUNTRY_CD = 'CA', SStSTRF1.COUNTY_TAX, 0) AS PST,
  IFF(L6.COUNTRY_CD = 'CA', SStSTRF1.STATE_TAX, 0) AS GST,
  SStSTRF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 SStSTRF1
  INNER JOIN LKPTRANS_6 L6 ON SStSTRF1.Monotonically_Increasing_Id = L6.Monotonically_Increasing_Id"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXPTRANS_7")

# COMMAND ----------
# DBTITLE 1, STORE_TAX_RATE


spark.sql("""INSERT INTO
  STORE_TAX_RATE
SELECT
  SITE_NBR AS SITE_NBR,
  COUNTRY_CD AS COUNTRY_CD,
  JURISDICTION_TAX AS JURISDICTION_TAX,
  CITY_TAX AS CITY_TAX,
  o_COUNTY_TAX AS COUNTY_TAX,
  o_STATE_TAX AS STATE_TAX,
  PST AS PST,
  GST AS GST
FROM
  EXPTRANS_7""")

# COMMAND ----------
# DBTITLE 1, StoreRetail


spark.sql("""INSERT INTO
  StoreRetail
SELECT
  FacilityGid AS FacilityGid,
  CityTaxRate AS CityTaxRate,
  CountyTaxRate AS CountyTaxRate,
  StateTaxRate AS StateTaxRate,
  JurisdictionTax AS JurisdictionTax,
  PSTRate AS PSTRate,
  GSTRate AS GSTRate
FROM
  EXP_StoreRetail_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_store_tax_rate")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_store_tax_rate", mainWorkflowId, parentName)
