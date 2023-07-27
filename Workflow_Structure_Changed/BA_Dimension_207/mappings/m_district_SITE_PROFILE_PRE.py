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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_district_SITE_PROFILE_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_district_SITE_PROFILE_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SITE_PROFILE_PRE_0


query_0 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  COMPANY_DESC AS COMPANY_DESC,
  COMPANY_ID AS COMPANY_ID,
  ADD_DT AS ADD_DT,
  CLOSE_DT AS CLOSE_DT,
  DELETE_DT AS DELETE_DT,
  OPEN_DT AS OPEN_DT,
  DISTRICT_DESC AS DISTRICT_DESC,
  DISTRICT_ID AS DISTRICT_ID,
  PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  REGION_DESC AS REGION_DESC,
  REGION_ID AS REGION_ID,
  REPL_DC_NBR AS REPL_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_NAME AS COUNTRY_NAME,
  STORE_NAME AS STORE_NAME,
  STORE_NBR AS STORE_NBR,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  STATE_CD AS STATE_CD,
  STORE_TYPE_DESC AS STORE_TYPE_DESC,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
  GR_OPEN_DT AS GR_OPEN_DT,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  BP_GL_ACCT AS BP_GL_ACCT,
  SITE_ADDRESS AS SITE_ADDRESS,
  POSTAL_CD AS POSTAL_CD,
  SITE_CITY AS SITE_CITY,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  LOAD_DT AS LOAD_DT,
  PROMO_LABEL_CD AS PROMO_LABEL_CD,
  SFT_OPEN_DT AS SFT_OPEN_DT,
  BANFIELD_FLAG AS BANFIELD_FLAG,
  SALES_AREA_FLR_SPC AS SALES_AREA_FLR_SPC,
  SITE_CATEGORY AS SITE_CATEGORY
FROM
  SITE_PROFILE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SITE_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1


query_1 = f"""SELECT
  A.DISTRICT_ID AS DISTRICT_ID,
  A.DISTRICT_DESC AS DISTRICT_DESC,
  NVL(B.DISTRICT_SITE_LOGIN_ID, '0') AS DISTRICT_SITE_LOGIN_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      DISTRICT_ID,
      NVL(MAX(DISTRICT_DESC), 'NOT DEFINED') DISTRICT_DESC
    FROM
      Shortcut_To_SITE_PROFILE_PRE_0
    GROUP BY
      DISTRICT_ID
  ) A
  LEFT JOIN (
    SELECT
      DISTRICT_ID,
      MAX(
        CASE
          RN
          WHEN 1 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 2 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 3 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 4 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 5 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 6 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 7 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 8 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 9 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 10 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 11 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 12 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 13 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 14 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 15 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 16 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 17 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 18 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 19 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 20 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 21 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 22 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 23 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 24 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 25 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 26 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 27 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 28 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 29 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 30 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 31 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 32 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 33 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 34 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 35 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 36 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 37 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 38 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 39 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 40 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 41 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 42 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 43 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 44 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 45 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 46 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 47 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 48 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 49 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) || MAX(
        CASE
          RN
          WHEN 50 THEN SITE_LOGIN_ID || ' '
          ELSE ''
        END
      ) DISTRICT_SITE_LOGIN_ID
    FROM
      (
        SELECT
          DISTRICT_ID,
          SITE_LOGIN_ID,
          ROW_NUMBER() OVER (
            PARTITION BY
              DISTRICT_ID
            ORDER BY
              STORE_OPEN_CLOSE_FLAG DESC,
              SITE_LOGIN_ID
          ) RN
        FROM
          SITE_PROFILE
        WHERE
          NVL(DISTRICT_ID, '0') <> '0'
          AND NVL(SITE_LOGIN_ID, '0') <> '0'
      ) A
    GROUP BY
      DISTRICT_ID
  ) B ON A.DISTRICT_ID = B.DISTRICT_ID"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC,
  DISTRICT_SITE_LOGIN_ID AS DISTRICT_SITE_LOGIN_ID,
  REPLACECHR(1, DISTRICT_SITE_LOGIN_ID, 'M', 'S') AS DISTRICT_SALON_LOGIN_ID,
  REPLACESTR(1, DISTRICT_SITE_LOGIN_ID, 'M', 'HM') AS DISTRICT_HOTEL_LOGIN_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, DISTRICT


spark.sql("""INSERT INTO
  DISTRICT
SELECT
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC,
  DISTRICT_SITE_LOGIN_ID AS DISTRICT_SITE_LOGIN_ID,
  DISTRICT_SALON_LOGIN_ID AS DISTRICT_SALON_LOGIN_ID,
  DISTRICT_HOTEL_LOGIN_ID AS DISTRICT_HOTEL_LOGIN_ID
FROM
  EXPTRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_district_SITE_PROFILE_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_district_SITE_PROFILE_PRE", mainWorkflowId, parentName)
