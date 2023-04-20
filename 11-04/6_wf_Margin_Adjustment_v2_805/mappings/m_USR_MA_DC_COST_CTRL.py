# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, USR_MA_DC_COST_CTRL_0


df_0=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        COMPANY_ID AS COMPANY_ID,
        FISCAL_MO AS FISCAL_MO,
        DC_COST_PCT AS DC_COST_PCT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_MA_DC_COST_CTRL""")

df_0.createOrReplaceTempView("USR_MA_DC_COST_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_MA_DC_COST_CTRL_1


df_1=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        COMPANY_ID AS COMPANY_ID,
        FISCAL_MO AS FISCAL_MO,
        DC_COST_PCT AS DC_COST_PCT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_MA_DC_COST_CTRL_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_MA_DC_COST_CTRL_1")

# COMMAND ----------
# DBTITLE 1, EXP_USR_MA_DC_COST_CTRL_2


df_2=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        COMPANY_ID AS COMPANY_ID,
        FISCAL_MO AS FISCAL_MO,
        DC_COST_PCT AS DC_COST_PCT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_USR_MA_DC_COST_CTRL_1""")

df_2.createOrReplaceTempView("EXP_USR_MA_DC_COST_CTRL_2")

# COMMAND ----------
# DBTITLE 1, USR_MA_DC_COST_CTRL


spark.sql("""INSERT INTO USR_MA_DC_COST_CTRL SELECT MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
COMPANY_ID AS COMPANY_ID,
FISCAL_MO AS FISCAL_MO,
DC_COST_PCT AS DC_COST_PCT FROM EXP_USR_MA_DC_COST_CTRL_2""")