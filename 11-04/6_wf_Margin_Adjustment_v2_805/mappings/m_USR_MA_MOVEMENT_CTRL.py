# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, USR_MA_MOVEMENT_CTRL_0


df_0=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        MA_AMT AS MA_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_MA_MOVEMENT_CTRL""")

df_0.createOrReplaceTempView("USR_MA_MOVEMENT_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_MA_MOVEMENT_CTRL_1


df_1=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        MA_AMT AS MA_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_MA_MOVEMENT_CTRL_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_MA_MOVEMENT_CTRL_1")

# COMMAND ----------
# DBTITLE 1, EXP_USR_MA_MOVEMENT_CTRL_2


df_2=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        MA_AMT AS MA_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_USR_MA_MOVEMENT_CTRL_1""")

df_2.createOrReplaceTempView("EXP_USR_MA_MOVEMENT_CTRL_2")

# COMMAND ----------
# DBTITLE 1, USR_MA_MOVEMENT_CTRL


spark.sql("""INSERT INTO USR_MA_MOVEMENT_CTRL SELECT MOVEMENT_ID AS MOVEMENT_ID,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
FISCAL_MO AS FISCAL_MO,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
GL_ACCT_NBR AS GL_ACCT_NBR,
MA_AMT AS MA_AMT FROM EXP_USR_MA_MOVEMENT_CTRL_2""")