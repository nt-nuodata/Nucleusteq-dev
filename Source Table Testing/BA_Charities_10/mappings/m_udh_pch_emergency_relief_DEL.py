# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_USR_PCH_EMERGENCY_RELIEF_v2_0


df_0 = spark.sql("""SELECT
  GRANT_REQ_ID_v2 AS GRANT_REQ_ID_v2,
  ER_DT AS ER_DT,
  ER_SEQ_NO AS ER_SEQ_NO,
  ER_RESCUE_TYPE_ID AS ER_RESCUE_TYPE_ID,
  ER_RESCUE_TYPE_DESC AS ER_RESCUE_TYPE_DESC,
  ER_SPECIES_ID AS ER_SPECIES_ID,
  ER_SPECIES_DESC AS ER_SPECIES_DESC,
  ER_PAYMENT_TYPE_ID AS ER_PAYMENT_TYPE_ID,
  ER_PAYMENT_TYPE_DESC AS ER_PAYMENT_TYPE_DESC,
  ER_STATE_CD AS ER_STATE_CD,
  ER_REGION AS ER_REGION,
  ER_DISTRICT AS ER_DISTRICT,
  STORE_NBR AS STORE_NBR,
  ANIMAL_CNT AS ANIMAL_CNT,
  REQUESTED_AMT AS REQUESTED_AMT,
  FUNDED_AMT AS FUNDED_AMT,
  USER_ID AS USER_ID,
  WEEK_DT AS WEEK_DT,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_YR AS FISCAL_YR,
  CAL_WK AS CAL_WK,
  CAL_MO AS CAL_MO,
  CAL_QTR AS CAL_QTR,
  CAL_YR AS CAL_YR,
  DELETED_IND AS DELETED_IND,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  USR_PCH_EMERGENCY_RELIEF_v2""")

df_0.createOrReplaceTempView("Shortcut_to_USR_PCH_EMERGENCY_RELIEF_v2_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_PCH_EMERGENCY_RELIEF_1


df_1 = spark.sql("""SELECT
  GRANT_REQ_ID_v2 AS ER_GRANT_ID,
  ER_DT AS ER_DT,
  ER_SEQ_NO AS ER_SEQ_NO,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_USR_PCH_EMERGENCY_RELIEF_v2_0
WHERE
  UPDATE_TSTMP > CURRENT_DATE""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_PCH_EMERGENCY_RELIEF_1")

# COMMAND ----------
# DBTITLE 1, UPD_DELETE_2


df_2 = spark.sql("""SELECT
  ER_GRANT_ID AS ER_GRANT_ID,
  ER_DT AS ER_DT,
  ER_SEQ_NO AS ER_SEQ_NO,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_USR_PCH_EMERGENCY_RELIEF_1""")

df_2.createOrReplaceTempView("UPD_DELETE_2")

# COMMAND ----------
# DBTITLE 1, UDH_PCH_EMERGENCY_RELIEF


spark.sql("""MERGE INTO UDH_PCH_EMERGENCY_RELIEF AS TARGET
USING
  UPD_DELETE_2 AS SOURCE ON TARGET.ER_SEQ_NO = SOURCE.ER_SEQ_NO
  AND TARGET.ER_GRANT_ID = SOURCE.ER_GRANT_ID
  AND TARGET.ER_DT = SOURCE.ER_DT
  WHEN MATCHED THEN DELETE""")