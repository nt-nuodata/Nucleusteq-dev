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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_profile", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, SEQ


spark.sql(
  """CREATE TABLE IF NOT EXISTS SEQ(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);"""
)

spark.sql(
  """INSERT INTO SEQ(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(1000001, 1000000, 1)"""
)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PROFILE_PRE1_1


query_1 = f"""SELECT
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_GROUP AS VENDOR_GROUP,
  SITE AS SITE,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
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
  FAX AS FAX,
  RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  RTV_TYPE_CD AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD AS INDUSTRY_CD,
  ADD_DT AS ADD_DT
FROM
  VENDOR_PROFILE_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_VENDOR_PROFILE_PRE1_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2


query_2 = f"""SELECT
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_GROUP AS VENDOR_GROUP,
  SITE AS SITE,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
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
  FAX AS FAX,
  RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  RTV_TYPE_CD AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD AS INDUSTRY_CD,
  ADD_DT AS ADD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_PROFILE_PRE1_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2")

# COMMAND ----------
# DBTITLE 1, EXP_SQ_3


query_3 = f"""SELECT
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_GROUP AS VENDOR_GROUP,
  SITE AS SITE,
  TO_Integer (LTRIM(SITE, '0')) AS STORE_NBR,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_ID,
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
  FAX AS FAX,
  RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  RTV_TYPE_CD AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD AS INDUSTRY_CD,
  ADD_DT AS ADD_DT,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_SQ_3")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_4


query_4 = f"""SELECT
  SP.LOCATION_ID AS LOCATION_ID,
  SP.STORE_TYPE_ID AS STORE_TYPE_ID,
  SP.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
  SP.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
  SP.TIME_ZONE_ID AS TIME_ZONE_ID,
  SP.STORE_NAME AS STORE_NAME,
  SP.CLOSE_DT AS CLOSE_DT,
  ES3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_SQ_3 ES3
  LEFT JOIN SITE_PROFILE SP ON SP.STORE_NBR = ES3.STORE_NBR"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_SITE_PROFILE_4")

# COMMAND ----------
# DBTITLE 1, EXP_FINAL_5


query_5 = f"""SELECT
  ES3.VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  ES3.VENDOR_NBR AS VENDOR_NBR,
  ES3.VENDOR_NAME AS VENDOR_NAME,
  ES3.VENDOR_GROUP AS VENDOR_GROUP,
  ES3.PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  ES3.EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
  ES3.PURCHASE_BLOCK AS PURCHASE_BLOCK,
  ES3.POSTING_BLOCK AS POSTING_BLOCK,
  ES3.DELETION_FLAG AS DELETION_FLAG,
  ES3.VIP_CD AS VIP_CD,
  ES3.INACTIVE_FLAG AS INACTIVE_FLAG,
  ES3.PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
  ES3.INCO_TERM_CD AS INCO_TERM_CD,
  ES3.ADDRESS AS ADDRESS,
  ES3.CITY AS CITY,
  ES3.STATE AS STATE,
  ES3.COUNTRY_CD AS COUNTRY_CD,
  ES3.ZIP AS ZIP,
  ES3.CONTACT AS CONTACT,
  ES3.CONTACT_PHONE AS CONTACT_PHONE,
  ES3.PHONE AS PHONE,
  ES3.FAX AS FAX,
  ES3.RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  ES3.RTV_TYPE_CD AS RTV_TYPE_CD,
  ES3.RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  ES3.INDUSTRY_CD AS INDUSTRY_CD,
  ES3.ADD_DT AS ADD_DT,
  LSP4.LOCATION_ID AS LOCATION_ID,
  LSP4.STORE_TYPE_ID AS STORE_TYPE_ID,
  LSP4.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
  LSP4.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
  ES3.STORE_NBR AS STORE_NBR,
  LSP4.TIME_ZONE_ID AS TIME_ZONE_ID,
  LSP4.STORE_NAME AS STORE_NAME,
  LSP4.CLOSE_DT AS CLOSE_DT,
  ES3.PURCH_GROUP_ID AS PURCH_GROUP_ID,
  ES3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_SQ_3 ES3
  INNER JOIN LKP_SITE_PROFILE_4 LSP4 ON ES3.Monotonically_Increasing_Id = LSP4.Monotonically_Increasing_Id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_FINAL_5")

# COMMAND ----------
# DBTITLE 1, LKP_VENDOR_PROFILE_6


query_6 = f"""SELECT
  VP.VENDOR_ID AS VENDOR_ID,
  EF5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_FINAL_5 EF5
  LEFT JOIN VENDOR_PROFILE VP ON VP.VENDOR_TYPE_ID = EF5.VENDOR_TYPE_ID
  AND VP.VENDOR_NBR = EF5.VENDOR_NBR"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("LKP_VENDOR_PROFILE_6")

# COMMAND ----------
# DBTITLE 1, RTR_VENDOR_TYPE


query_7 = f"""SELECT
  LVP6.VENDOR_ID AS VENDOR_ID1,
  EF5.VENDOR_TYPE_ID AS VENDOR_TYPE_ID1,
  EF5.VENDOR_NBR AS VENDOR_NBR1,
  EF5.STORE_TYPE_ID AS STORE_TYPE_ID1,
  EF5.VENDOR_NAME AS VENDOR_NAME1,
  EF5.VENDOR_GROUP AS VENDOR_GROUP1,
  EF5.PARENT_VENDOR_ID AS PARENT_VENDOR_ID1,
  EF5.EDI_ELIG_FLAG AS EDI_ELIG_FLAG1,
  EF5.PURCHASE_BLOCK AS PURCHASE_BLOCK1,
  EF5.POSTING_BLOCK AS POSTING_BLOCK1,
  EF5.DELETION_FLAG AS DELETION_FLAG1,
  EF5.VIP_CD AS VIP_CD1,
  EF5.INACTIVE_FLAG AS INACTIVE_FLAG1,
  EF5.PAYMENT_TERM_CD AS PAYMENT_TERM_CD1,
  EF5.INCO_TERM_CD AS INCO_TERM_CD1,
  EF5.ADDRESS AS ADDRESS1,
  EF5.CITY AS CITY1,
  EF5.STATE AS STATE1,
  EF5.COUNTRY_CD AS COUNTRY_CD1,
  EF5.ZIP AS ZIP1,
  EF5.CONTACT AS CONTACT1,
  EF5.CONTACT_PHONE AS CONTACT_PHONE1,
  EF5.PHONE AS PHONE1,
  EF5.FAX AS FAX1,
  EF5.RTV_ELIG_FLAG AS RTV_ELIG_FLAG1,
  EF5.RTV_TYPE_CD AS RTV_TYPE_CD1,
  EF5.RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD1,
  EF5.INDUSTRY_CD AS INDUSTRY_CD1,
  EF5.ADD_DT AS ADD_DT1,
  EF5.LOCATION_ID AS LOCATION_ID1,
  EF5.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR1,
  EF5.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR1,
  EF5.STORE_NBR AS STORE_NBR1,
  EF5.TIME_ZONE_ID AS TIME_ZONE_ID1,
  EF5.STORE_NAME AS STORE_NAME1,
  EF5.CLOSE_DT AS CLOSE_DT1,
  EF5.PURCH_GROUP_ID AS PURCH_GROUP_ID1,
  LVP6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_VENDOR_PROFILE_6 LVP6
  INNER JOIN EXP_FINAL_5 EF5 ON LVP6.Monotonically_Increasing_Id = EF5.Monotonically_Increasing_Id
WHERE
  EF5.VENDOR_TYPE_ID = 2"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_VENDOR_TYPE_MERCH_7")

query_8 = f"""SELECT
  LVP6.VENDOR_ID AS VENDOR_ID3,
  EF5.VENDOR_TYPE_ID AS VENDOR_TYPE_ID3,
  EF5.VENDOR_NBR AS VENDOR_NBR3,
  EF5.STORE_TYPE_ID AS STORE_TYPE_ID3,
  EF5.VENDOR_NAME AS VENDOR_NAME3,
  EF5.VENDOR_GROUP AS VENDOR_GROUP3,
  EF5.PARENT_VENDOR_ID AS PARENT_VENDOR_ID3,
  EF5.EDI_ELIG_FLAG AS EDI_ELIG_FLAG3,
  EF5.PURCHASE_BLOCK AS PURCHASE_BLOCK3,
  EF5.POSTING_BLOCK AS POSTING_BLOCK3,
  EF5.DELETION_FLAG AS DELETION_FLAG3,
  EF5.VIP_CD AS VIP_CD3,
  EF5.INACTIVE_FLAG AS INACTIVE_FLAG3,
  EF5.PAYMENT_TERM_CD AS PAYMENT_TERM_CD3,
  EF5.INCO_TERM_CD AS INCO_TERM_CD3,
  EF5.ADDRESS AS ADDRESS3,
  EF5.CITY AS CITY3,
  EF5.STATE AS STATE3,
  EF5.COUNTRY_CD AS COUNTRY_CD3,
  EF5.ZIP AS ZIP3,
  EF5.CONTACT AS CONTACT3,
  EF5.CONTACT_PHONE AS CONTACT_PHONE3,
  EF5.PHONE AS PHONE3,
  EF5.FAX AS FAX3,
  EF5.RTV_ELIG_FLAG AS RTV_ELIG_FLAG3,
  EF5.RTV_TYPE_CD AS RTV_TYPE_CD3,
  EF5.RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD3,
  EF5.INDUSTRY_CD AS INDUSTRY_CD3,
  EF5.ADD_DT AS ADD_DT3,
  EF5.LOCATION_ID AS LOCATION_ID3,
  EF5.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR3,
  EF5.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR3,
  EF5.STORE_NBR AS STORE_NBR3,
  EF5.TIME_ZONE_ID AS TIME_ZONE_ID3,
  EF5.STORE_NAME AS STORE_NAME3,
  EF5.CLOSE_DT AS CLOSE_DT3,
  EF5.PURCH_GROUP_ID AS PURCH_GROUP_ID3,
  LVP6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_VENDOR_PROFILE_6 LVP6
  INNER JOIN EXP_FINAL_5 EF5 ON LVP6.Monotonically_Increasing_Id = EF5.Monotonically_Increasing_Id
WHERE
  ISNULL (LVP6.VENDOR_ID)
  AND EF5.VENDOR_TYPE_ID <> 2"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("FIL_VENDOR_TYPE_NON_MERCH_8")

query_9 = f"""SELECT
  LVP6.VENDOR_ID AS VENDOR_ID4,
  EF5.VENDOR_TYPE_ID AS VENDOR_TYPE_ID4,
  EF5.VENDOR_NBR AS VENDOR_NBR4,
  EF5.STORE_TYPE_ID AS STORE_TYPE_ID4,
  EF5.VENDOR_NAME AS VENDOR_NAME4,
  EF5.VENDOR_GROUP AS VENDOR_GROUP4,
  EF5.PARENT_VENDOR_ID AS PARENT_VENDOR_ID4,
  EF5.EDI_ELIG_FLAG AS EDI_ELIG_FLAG4,
  EF5.PURCHASE_BLOCK AS PURCHASE_BLOCK4,
  EF5.POSTING_BLOCK AS POSTING_BLOCK4,
  EF5.DELETION_FLAG AS DELETION_FLAG4,
  EF5.VIP_CD AS VIP_CD4,
  EF5.INACTIVE_FLAG AS INACTIVE_FLAG4,
  EF5.PAYMENT_TERM_CD AS PAYMENT_TERM_CD4,
  EF5.INCO_TERM_CD AS INCO_TERM_CD4,
  EF5.ADDRESS AS ADDRESS4,
  EF5.CITY AS CITY4,
  EF5.STATE AS STATE4,
  EF5.COUNTRY_CD AS COUNTRY_CD4,
  EF5.ZIP AS ZIP4,
  EF5.CONTACT AS CONTACT4,
  EF5.CONTACT_PHONE AS CONTACT_PHONE4,
  EF5.PHONE AS PHONE4,
  EF5.FAX AS FAX4,
  EF5.RTV_ELIG_FLAG AS RTV_ELIG_FLAG4,
  EF5.RTV_TYPE_CD AS RTV_TYPE_CD4,
  EF5.RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD4,
  EF5.INDUSTRY_CD AS INDUSTRY_CD4,
  EF5.ADD_DT AS ADD_DT4,
  EF5.LOCATION_ID AS LOCATION_ID4,
  EF5.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR4,
  EF5.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR4,
  EF5.STORE_NBR AS STORE_NBR4,
  EF5.TIME_ZONE_ID AS TIME_ZONE_ID4,
  EF5.STORE_NAME AS STORE_NAME4,
  EF5.CLOSE_DT AS CLOSE_DT4,
  EF5.PURCH_GROUP_ID AS PURCH_GROUP_ID4,
  LVP6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_VENDOR_PROFILE_6 LVP6
  INNER JOIN EXP_FINAL_5 EF5 ON LVP6.Monotonically_Increasing_Id = EF5.Monotonically_Increasing_Id
WHERE
  ISNULL (LVP6.VENDOR_ID)
  AND EF5.VENDOR_TYPE_ID = 10
  AND IN(EF5.STORE_TYPE_ID, '100', 'MIX', 'WHL', 0) = 1"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("FIL_VENDOR_TYPE_DC_9")

# COMMAND ----------
# DBTITLE 1, EXP_DC_10


query_10 = f"""SELECT
  STORE_NBR4 AS STORE_NBR4,
  STORE_NBR4 + 900000 AS VENDOR_ID4,
  STORE_NAME4 AS STORE_NAME4,
  STORE_NAME4 AS VENDOR_NAME,
  10 AS VENDOR_TYPE_ID4,
  'V' || TO_CHAR(LPAD(STORE_NBR4, 4, 0)) AS VENDOR_NBR4,
  LOCATION_ID4 AS LOCATION_ID4,
  PARENT_VENDOR_ID4 AS PARENT_VENDOR_ID4,
  'N' AS EDI_ELIG_FLAG4,
  ADDRESS4 AS ADDRESS4,
  CITY4 AS CITY4,
  STATE4 AS STATE4,
  COUNTRY_CD4 AS COUNTRY_CD4,
  ZIP4 AS ZIP4,
  PHONE4 AS PHONE4,
  GEO_LATITUDE_NBR4 AS GEO_LATITUDE_NBR4,
  GEO_LONGITUDE_NBR4 AS GEO_LONGITUDE_NBR4,
  ADD_DT4 AS ADD_DT4,
  TIME_ZONE_ID4 AS TIME_ZONE_ID4,
  CLOSE_DT4 AS CLOSE_DT4,
  SESSSTARTTIME AS LOAD_DT,
  PURCH_GROUP_ID4 AS PURCH_GROUP_ID4,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_VENDOR_TYPE_DC_9"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("EXP_DC_10")

# COMMAND ----------
# DBTITLE 1, UPDATE_DC_INSERT_11


query_11 = f"""SELECT
  VENDOR_ID4 AS VENDOR_ID4,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_TYPE_ID4 AS VENDOR_TYPE_ID4,
  VENDOR_NBR4 AS VENDOR_NBR4,
  LOCATION_ID4 AS LOCATION_ID4,
  PARENT_VENDOR_ID4 AS PARENT_VENDOR_ID4,
  EDI_ELIG_FLAG4 AS EDI_ELIG_FLAG4,
  ADDRESS4 AS ADDRESS4,
  CITY4 AS CITY4,
  STATE4 AS STATE4,
  COUNTRY_CD4 AS COUNTRY_CD4,
  ZIP4 AS ZIP4,
  PHONE4 AS PHONE4,
  GEO_LATITUDE_NBR4 AS GEO_LATITUDE_NBR4,
  GEO_LONGITUDE_NBR4 AS GEO_LONGITUDE_NBR4,
  ADD_DT4 AS ADD_DT4,
  TIME_ZONE_ID4 AS TIME_ZONE_ID4,
  CLOSE_DT4 AS CLOSE_DT4,
  LOAD_DT AS LOAD_DT,
  PURCH_GROUP_ID4 AS PURCH_GROUP_ID4,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DC_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("UPDATE_DC_INSERT_11")

# COMMAND ----------
# DBTITLE 1, EXP_MERCH_12


query_12 = f"""SELECT
  VENDOR_NBR1 AS VENDOR_NBR1,
  TO_INTEGER(VENDOR_NBR1) AS VENDOR_ID,
  VENDOR_ID1 AS LKP_VENDOR_ID1,
  VENDOR_TYPE_ID1 AS VENDOR_TYPE_ID1,
  STORE_TYPE_ID1 AS STORE_TYPE_ID1,
  VENDOR_NAME1 AS VENDOR_NAME1,
  VENDOR_GROUP1 AS VENDOR_GROUP1,
  PARENT_VENDOR_ID1 AS PARENT_VENDOR_ID1,
  EDI_ELIG_FLAG1 AS EDI_ELIG_FLAG1,
  PURCHASE_BLOCK1 AS PURCHASE_BLOCK1,
  POSTING_BLOCK1 AS POSTING_BLOCK1,
  DELETION_FLAG1 AS DELETION_FLAG,
  VIP_CD1 AS VIP_CD,
  INACTIVE_FLAG1 AS INACTIVE_FLAG1,
  PAYMENT_TERM_CD1 AS PAYMENT_TERM_CD1,
  INCO_TERM_CD1 AS INCO_TERM_CD1,
  ADDRESS1 AS ADDRESS1,
  CITY1 AS CITY1,
  STATE1 AS STATE1,
  COUNTRY_CD1 AS COUNTRY_CD1,
  ZIP1 AS ZIP1,
  CONTACT1 AS CONTACT1,
  CONTACT_PHONE1 AS CONTACT_PHONE1,
  PHONE1 AS PHONE1,
  FAX1 AS FAX1,
  RTV_ELIG_FLAG1 AS RTV_ELIG_FLAG1,
  RTV_TYPE_CD1 AS RTV_TYPE_CD1,
  RTV_FREIGHT_TYPE_CD1 AS RTV_FREIGHT_TYPE_CD1,
  INDUSTRY_CD1 AS INDUSTRY_CD1,
  ADD_DT1 AS ADD_DT1,
  LOCATION_ID1 AS LOCATION_ID1,
  SESSSTARTTIME AS LOAD_DT,
  PURCH_GROUP_ID1 AS PURCH_GROUP_ID1,
  IFF (ISNULL(VENDOR_ID1), 0, 1) AS UpdateStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_VENDOR_TYPE_MERCH_7"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("EXP_MERCH_12")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_13


query_13 = f"""SELECT
  LKP_VENDOR_ID1 AS LKP_VENDOR_ID1,
  VENDOR_NBR1 AS VENDOR_NBR1,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_TYPE_ID1 AS VENDOR_TYPE_ID1,
  VENDOR_NAME1 AS VENDOR_NAME1,
  EDI_ELIG_FLAG1 AS EDI_ELIG_FLAG1,
  PURCHASE_BLOCK1 AS PURCHASE_BLOCK1,
  POSTING_BLOCK1 AS POSTING_BLOCK1,
  DELETION_FLAG AS DELETION_FLAG,
  VIP_CD AS VIP_CD,
  INACTIVE_FLAG1 AS INACTIVE_FLAG1,
  PAYMENT_TERM_CD1 AS PAYMENT_TERM_CD1,
  INCO_TERM_CD1 AS INCO_TERM_CD1,
  ADDRESS1 AS ADDRESS1,
  CITY1 AS CITY1,
  STATE1 AS STATE1,
  COUNTRY_CD1 AS COUNTRY_CD1,
  ZIP1 AS ZIP1,
  CONTACT1 AS CONTACT1,
  CONTACT_PHONE1 AS CONTACT_PHONE1,
  PHONE1 AS PHONE1,
  FAX1 AS FAX1,
  RTV_ELIG_FLAG1 AS RTV_ELIG_FLAG1,
  RTV_TYPE_CD1 AS RTV_TYPE_CD1,
  RTV_FREIGHT_TYPE_CD1 AS RTV_FREIGHT_TYPE_CD1,
  INDUSTRY_CD1 AS INDUSTRY_CD1,
  ADD_DT1 AS ADD_DT1,
  LOCATION_ID1 AS LOCATION_ID1,
  LOAD_DT AS LOAD_DT,
  PURCH_GROUP_ID1 AS PURCH_GROUP_ID1,
  UpdateStrategy AS UpdateStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UpdateStrategy AS UPDATE_STRATEGY_FLAG
FROM
  EXP_MERCH_12"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("UPD_INS_UPD_13")

# COMMAND ----------
# DBTITLE 1, EXP_NON_MERCH_14


query_14 = f"""SELECT
  VENDOR_TYPE_ID3 AS VENDOR_TYPE_ID3,
  VENDOR_NBR3 AS VENDOR_NBR3,
  STORE_TYPE_ID3 AS STORE_TYPE_ID3,
  VENDOR_NAME3 AS VENDOR_NAME3,
  VENDOR_GROUP3 AS VENDOR_GROUP3,
  PARENT_VENDOR_ID3 AS PARENT_VENDOR_ID3,
  EDI_ELIG_FLAG3 AS EDI_ELIG_FLAG3,
  PURCHASE_BLOCK3 AS PURCHASE_BLOCK3,
  POSTING_BLOCK3 AS POSTING_BLOCK3,
  DELETION_FLAG3 AS DELETION_FLAG,
  VIP_CD3 AS VIP_CD,
  INACTIVE_FLAG3 AS INACTIVE_FLAG3,
  PAYMENT_TERM_CD3 AS PAYMENT_TERM_CD3,
  INCO_TERM_CD3 AS INCO_TERM_CD3,
  ADDRESS3 AS ADDRESS3,
  CITY3 AS CITY3,
  STATE3 AS STATE3,
  COUNTRY_CD3 AS COUNTRY_CD3,
  ZIP3 AS ZIP3,
  CONTACT3 AS CONTACT3,
  CONTACT_PHONE3 AS CONTACT_PHONE3,
  PHONE3 AS PHONE3,
  FAX3 AS FAX3,
  RTV_ELIG_FLAG3 AS RTV_ELIG_FLAG3,
  RTV_TYPE_CD3 AS RTV_TYPE_CD3,
  RTV_FREIGHT_TYPE_CD3 AS RTV_FREIGHT_TYPE_CD3,
  INDUSTRY_CD3 AS INDUSTRY_CD3,
  ADD_DT3 AS ADD_DT3,
  LOCATION_ID3 AS LOCATION_ID3,
  PURCH_GROUP_ID3 AS PURCH_GROUP_ID3,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_VENDOR_TYPE_NON_MERCH_8"""

df_14 = spark.sql(query_14)

df_14.createOrReplaceTempView("EXP_NON_MERCH_14")

# COMMAND ----------
# DBTITLE 1, UPD_NON_MERCH_INSERT_15


query_15 = f"""SELECT
  VENDOR_TYPE_ID3 AS VENDOR_TYPE_ID3,
  VENDOR_NBR3 AS VENDOR_NBR3,
  VENDOR_NAME3 AS VENDOR_NAME3,
  EDI_ELIG_FLAG3 AS EDI_ELIG_FLAG3,
  PURCHASE_BLOCK3 AS PURCHASE_BLOCK3,
  POSTING_BLOCK3 AS POSTING_BLOCK3,
  DELETION_FLAG AS DELETION_FLAG,
  INACTIVE_FLAG3 AS INACTIVE_FLAG3,
  PAYMENT_TERM_CD3 AS PAYMENT_TERM_CD3,
  INCO_TERM_CD3 AS INCO_TERM_CD3,
  ADDRESS3 AS ADDRESS3,
  CITY3 AS CITY3,
  STATE3 AS STATE3,
  COUNTRY_CD3 AS COUNTRY_CD3,
  ZIP3 AS ZIP3,
  CONTACT3 AS CONTACT3,
  CONTACT_PHONE3 AS CONTACT_PHONE3,
  PHONE3 AS PHONE3,
  RTV_ELIG_FLAG3 AS RTV_ELIG_FLAG3,
  RTV_TYPE_CD3 AS RTV_TYPE_CD3,
  RTV_FREIGHT_TYPE_CD3 AS RTV_FREIGHT_TYPE_CD3,
  INDUSTRY_CD3 AS INDUSTRY_CD3,
  ADD_DT3 AS ADD_DT3,
  LOCATION_ID3 AS LOCATION_ID3,
  PURCH_GROUP_ID3 AS PURCH_GROUP_ID3,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_NON_MERCH_14"""

df_15 = spark.sql(query_15)

df_15.createOrReplaceTempView("UPD_NON_MERCH_INSERT_15")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE


spark.sql("""MERGE INTO VENDOR_PROFILE AS TARGET
USING
  UPD_INS_UPD_13 AS SOURCE ON TARGET.VENDOR_ID = SOURCE.VENDOR_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.VENDOR_ID = SOURCE.VENDOR_ID,
  TARGET.VENDOR_NAME = SOURCE.VENDOR_NAME1,
  TARGET.VENDOR_TYPE_ID = SOURCE.VENDOR_TYPE_ID1,
  TARGET.VENDOR_NBR = SOURCE.VENDOR_NBR1,
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID1,
  TARGET.PURCH_GROUP_ID = SOURCE.PURCH_GROUP_ID1,
  TARGET.EDI_ELIG_FLAG = SOURCE.EDI_ELIG_FLAG1,
  TARGET.PURCHASE_BLOCK = SOURCE.PURCHASE_BLOCK1,
  TARGET.POSTING_BLOCK = SOURCE.POSTING_BLOCK1,
  TARGET.DELETION_FLAG = SOURCE.DELETION_FLAG,
  TARGET.INACTIVE_FLAG = SOURCE.INACTIVE_FLAG1,
  TARGET.PAYMENT_TERM_CD = SOURCE.PAYMENT_TERM_CD1,
  TARGET.INCO_TERM_CD = SOURCE.INCO_TERM_CD1,
  TARGET.ADDRESS = SOURCE.ADDRESS1,
  TARGET.CITY = SOURCE.CITY1,
  TARGET.STATE = SOURCE.STATE1,
  TARGET.COUNTRY_CD = SOURCE.COUNTRY_CD1,
  TARGET.ZIP = SOURCE.ZIP1,
  TARGET.PHONE = SOURCE.PHONE1,
  TARGET.FAX = SOURCE.FAX1,
  TARGET.RTV_ELIG_FLAG = SOURCE.RTV_ELIG_FLAG1,
  TARGET.RTV_TYPE_CD = SOURCE.RTV_TYPE_CD1,
  TARGET.RTV_FREIGHT_TYPE_CD = SOURCE.RTV_FREIGHT_TYPE_CD1,
  TARGET.INDUSTRY_CD = SOURCE.INDUSTRY_CD1,
  TARGET.ADD_DT = SOURCE.ADD_DT1,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.VENDOR_NAME = SOURCE.VENDOR_NAME1
  AND TARGET.VENDOR_TYPE_ID = SOURCE.VENDOR_TYPE_ID1
  AND TARGET.VENDOR_NBR = SOURCE.VENDOR_NBR1
  AND TARGET.LOCATION_ID = SOURCE.LOCATION_ID1
  AND TARGET.PURCH_GROUP_ID = SOURCE.PURCH_GROUP_ID1
  AND TARGET.EDI_ELIG_FLAG = SOURCE.EDI_ELIG_FLAG1
  AND TARGET.PURCHASE_BLOCK = SOURCE.PURCHASE_BLOCK1
  AND TARGET.POSTING_BLOCK = SOURCE.POSTING_BLOCK1
  AND TARGET.DELETION_FLAG = SOURCE.DELETION_FLAG
  AND TARGET.INACTIVE_FLAG = SOURCE.INACTIVE_FLAG1
  AND TARGET.PAYMENT_TERM_CD = SOURCE.PAYMENT_TERM_CD1
  AND TARGET.INCO_TERM_CD = SOURCE.INCO_TERM_CD1
  AND TARGET.ADDRESS = SOURCE.ADDRESS1
  AND TARGET.CITY = SOURCE.CITY1
  AND TARGET.STATE = SOURCE.STATE1
  AND TARGET.COUNTRY_CD = SOURCE.COUNTRY_CD1
  AND TARGET.ZIP = SOURCE.ZIP1
  AND TARGET.PHONE = SOURCE.PHONE1
  AND TARGET.FAX = SOURCE.FAX1
  AND TARGET.RTV_ELIG_FLAG = SOURCE.RTV_ELIG_FLAG1
  AND TARGET.RTV_TYPE_CD = SOURCE.RTV_TYPE_CD1
  AND TARGET.RTV_FREIGHT_TYPE_CD = SOURCE.RTV_FREIGHT_TYPE_CD1
  AND TARGET.INDUSTRY_CD = SOURCE.INDUSTRY_CD1
  AND TARGET.ADD_DT = SOURCE.ADD_DT1
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.VENDOR_ID,
    TARGET.VENDOR_NAME,
    TARGET.VENDOR_TYPE_ID,
    TARGET.VENDOR_NBR,
    TARGET.LOCATION_ID,
    TARGET.PURCH_GROUP_ID,
    TARGET.EDI_ELIG_FLAG,
    TARGET.PURCHASE_BLOCK,
    TARGET.POSTING_BLOCK,
    TARGET.DELETION_FLAG,
    TARGET.INACTIVE_FLAG,
    TARGET.PAYMENT_TERM_CD,
    TARGET.INCO_TERM_CD,
    TARGET.ADDRESS,
    TARGET.CITY,
    TARGET.STATE,
    TARGET.COUNTRY_CD,
    TARGET.ZIP,
    TARGET.PHONE,
    TARGET.FAX,
    TARGET.RTV_ELIG_FLAG,
    TARGET.RTV_TYPE_CD,
    TARGET.RTV_FREIGHT_TYPE_CD,
    TARGET.INDUSTRY_CD,
    TARGET.ADD_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.VENDOR_ID,
    SOURCE.VENDOR_NAME1,
    SOURCE.VENDOR_TYPE_ID1,
    SOURCE.VENDOR_NBR1,
    SOURCE.LOCATION_ID1,
    SOURCE.PURCH_GROUP_ID1,
    SOURCE.EDI_ELIG_FLAG1,
    SOURCE.PURCHASE_BLOCK1,
    SOURCE.POSTING_BLOCK1,
    SOURCE.DELETION_FLAG,
    SOURCE.INACTIVE_FLAG1,
    SOURCE.PAYMENT_TERM_CD1,
    SOURCE.INCO_TERM_CD1,
    SOURCE.ADDRESS1,
    SOURCE.CITY1,
    SOURCE.STATE1,
    SOURCE.COUNTRY_CD1,
    SOURCE.ZIP1,
    SOURCE.PHONE1,
    SOURCE.FAX1,
    SOURCE.RTV_ELIG_FLAG1,
    SOURCE.RTV_TYPE_CD1,
    SOURCE.RTV_FREIGHT_TYPE_CD1,
    SOURCE.INDUSTRY_CD1,
    SOURCE.ADD_DT1,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE


spark.sql("""INSERT INTO
  VENDOR_PROFILE
SELECT
  VENDOR_ID4 AS VENDOR_ID,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_TYPE_ID4 AS VENDOR_TYPE_ID,
  VENDOR_NBR4 AS VENDOR_NBR,
  LOCATION_ID4 AS LOCATION_ID,
  NULL AS SUPERIOR_VENDOR_ID,
  PARENT_VENDOR_ID4 AS PARENT_VENDOR_ID,
  NULL AS PARENT_VENDOR_NAME,
  PURCH_GROUP_ID4 AS PURCH_GROUP_ID,
  EDI_ELIG_FLAG4 AS EDI_ELIG_FLAG,
  NULL AS PURCHASE_BLOCK,
  NULL AS POSTING_BLOCK,
  NULL AS DELETION_FLAG,
  NULL AS VIP_CD,
  NULL AS INACTIVE_FLAG,
  NULL AS PAYMENT_TERM_CD,
  NULL AS INCO_TERM_CD,
  ADDRESS4 AS ADDRESS,
  CITY4 AS CITY,
  STATE4 AS STATE,
  COUNTRY_CD4 AS COUNTRY_CD,
  ZIP4 AS ZIP,
  NULL AS CONTACT,
  NULL AS CONTACT_PHONE,
  PHONE4 AS PHONE,
  NULL AS PHONE_EXT,
  NULL AS FAX,
  NULL AS RTV_ELIG_FLAG,
  NULL AS RTV_TYPE_CD,
  NULL AS RTV_FREIGHT_TYPE_CD,
  NULL AS INDUSTRY_CD,
  GEO_LATITUDE_NBR4 AS LATITUDE,
  GEO_LONGITUDE_NBR4 AS LONGITUDE,
  TIME_ZONE_ID4 AS TIME_ZONE_ID,
  ADD_DT4 AS ADD_DT,
  CLOSE_DT4 AS UPDATE_DT,
  LOAD_DT AS LOAD_DT FROMUPDATE_DC_INSERT_11""")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE


spark.sql("""INSERT INTO
  VENDOR_PROFILE
SELECT
  NULL AS VENDOR_ID,
  VENDOR_NAME3 AS VENDOR_NAME,
  VENDOR_TYPE_ID3 AS VENDOR_TYPE_ID,
  VENDOR_NBR3 AS VENDOR_NBR,
  LOCATION_ID3 AS LOCATION_ID,
  NULL AS SUPERIOR_VENDOR_ID,
  NULL AS PARENT_VENDOR_ID,
  NULL AS PARENT_VENDOR_NAME,
  PURCH_GROUP_ID3 AS PURCH_GROUP_ID,
  EDI_ELIG_FLAG3 AS EDI_ELIG_FLAG,
  PURCHASE_BLOCK3 AS PURCHASE_BLOCK,
  POSTING_BLOCK3 AS POSTING_BLOCK,
  DELETION_FLAG AS DELETION_FLAG,
  NULL AS VIP_CD,
  INACTIVE_FLAG3 AS INACTIVE_FLAG,
  PAYMENT_TERM_CD3 AS PAYMENT_TERM_CD,
  INCO_TERM_CD3 AS INCO_TERM_CD,
  ADDRESS3 AS ADDRESS,
  CITY3 AS CITY,
  STATE3 AS STATE,
  COUNTRY_CD3 AS COUNTRY_CD,
  ZIP3 AS ZIP,
  CONTACT3 AS CONTACT,
  CONTACT_PHONE3 AS CONTACT_PHONE,
  PHONE3 AS PHONE,
  NULL AS PHONE_EXT,
  NULL AS FAX,
  RTV_ELIG_FLAG3 AS RTV_ELIG_FLAG,
  RTV_TYPE_CD3 AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD3 AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD3 AS INDUSTRY_CD,
  NULL AS LATITUDE,
  NULL AS LONGITUDE,
  NULL AS TIME_ZONE_ID,
  ADD_DT3 AS ADD_DT,
  NULL AS UPDATE_DT,
  NULL AS LOAD_DT FROMUPD_NON_MERCH_INSERT_15""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_profile")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_profile", mainWorkflowId, parentName)