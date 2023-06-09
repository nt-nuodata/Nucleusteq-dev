# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PCH_RESCUE_WAGON_0


df_0 = spark.sql("""SELECT
  RW_TRANS_DT AS RW_TRANS_DT,
  RW_ROUTE_ID AS RW_ROUTE_ID,
  SOURCE_SHELTER AS SOURCE_SHELTER,
  DESTINATION_SHELTER AS DESTINATION_SHELTER,
  NO_OF_PETS_TRANSPORTED AS NO_OF_PETS_TRANSPORTED,
  NO_OF_PUPPIES AS NO_OF_PUPPIES,
  NO_OF_DOGS AS NO_OF_DOGS,
  ACTION AS ACTION,
  USER_ID AS USER_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  UDH_PCH_RESCUE_WAGON""")

df_0.createOrReplaceTempView("Shortcut_to_UDH_PCH_RESCUE_WAGON_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PCH_RESCUE_WAGON_1


df_1 = spark.sql("""SELECT
  RW_TRANS_DT AS RW_TRANS_DT,
  RW_ROUTE_ID AS RW_ROUTE_ID,
  ACTION AS ACTION,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PCH_RESCUE_WAGON_0
WHERE
  ACTION = 'Delete'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PCH_RESCUE_WAGON_1")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_2


df_2 = spark.sql("""SELECT
  RW_TRANS_DT AS RW_TRANS_DT,
  RW_ROUTE_ID AS RW_ROUTE_ID,
  1 AS DELETED_IND,
  SESSSTARTTIME AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PCH_RESCUE_WAGON_1""")

df_2.createOrReplaceTempView("EXP_TRANS_2")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_3


df_3 = spark.sql("""SELECT
  RW_TRANS_DT AS RW_TRANS_DT,
  RW_ROUTE_ID AS RW_ROUTE_ID,
  DELETED_IND AS DELETED_IND,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_TRANS_2""")

df_3.createOrReplaceTempView("UPD_UPDATE_3")

# COMMAND ----------
# DBTITLE 1, USR_PCH_RESCUE_WAGON_v2


spark.sql("""MERGE INTO USR_PCH_RESCUE_WAGON_v2 AS TARGET
USING
  UPD_UPDATE_3 AS SOURCE ON TARGET.RW_ROUTE_ID = SOURCE.RW_ROUTE_ID
  AND TARGET.RW_TRANS_DT_v2 = SOURCE.RW_TRANS_DT
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.RW_TRANS_DT_v2 = SOURCE.RW_TRANS_DT,
  TARGET.RW_ROUTE_ID = SOURCE.RW_ROUTE_ID,
  TARGET.DELETED_IND = SOURCE.DELETED_IND,
  TARGET.UPDATE_TSTMP = SOURCE.LOAD_TSTMP""")