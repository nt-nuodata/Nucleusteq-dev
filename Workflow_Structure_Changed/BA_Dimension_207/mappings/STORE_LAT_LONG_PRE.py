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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "STORE_LAT_LONG_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"STORE_LAT_LONG_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Address_0


query_0 = f"""SELECT
  AddressId AS AddressId,
  StreetLine1 AS StreetLine1,
  StreetLine2 AS StreetLine2,
  City AS City,
  StateProvinceId AS StateProvinceId,
  ZipCode AS ZipCode,
  Location AS Location,
  Latitude AS Latitude,
  Longitude AS Longitude
FROM
  Address"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Address_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Store_1


query_1 = f"""SELECT
  StoreId AS StoreId,
  StoreNumber AS StoreNumber,
  AddressId AS AddressId,
  DistrictId AS DistrictId,
  Name AS Name,
  PhoneNumber AS PhoneNumber,
  IsActive AS IsActive,
  IsRelocation AS IsRelocation,
  OpeningDate AS OpeningDate,
  SoftLaunchDate AS SoftLaunchDate,
  InStorePickupEffectiveDate AS InStorePickupEffectiveDate,
  PhotoId AS PhotoId,
  NeedsReview AS NeedsReview,
  CreatedBy AS CreatedBy,
  CreatedDate AS CreatedDate,
  LastModifiedBy AS LastModifiedBy,
  LastModifiedDate AS LastModifiedDate,
  IsConcept AS IsConcept,
  GooglePlaceId AS GooglePlaceId
FROM
  Store"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_Store_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Address_2


query_2 = f"""SELECT
  a.latitude,
  a.longitude,
  s.storenumber
FROM
  Shortcut_to_Store_1 s
  INNER JOIN Shortcut_to_Address_0 a ON s.addressid = a.addressid
ORDER BY
  s.storenumber"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_Address_2")

# COMMAND ----------
# DBTITLE 1, STORE_LAT_LONG_PRE


spark.sql("""INSERT INTO
  STORE_LAT_LONG_PRE
SELECT
  StoreNumber AS STORE_NBR,
  Latitude AS LAT,
  Longitude AS LON
FROM
  SQ_Shortcut_to_Address_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "STORE_LAT_LONG_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "STORE_LAT_LONG_PRE", mainWorkflowId, parentName)
