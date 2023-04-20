# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_MOVEMENT_PRE_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        PO_NBR AS PO_NBR,
        PO_LINE_NBR AS PO_LINE_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        STO_TYPE_ID AS STO_TYPE_ID,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_MOVEMENT_PRE""")

df_0.createOrReplaceTempView("MA_MOVEMENT_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_MOVEMENT_PRE_1


df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        PO_NBR AS PO_NBR,
        PO_LINE_NBR AS PO_LINE_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        STO_TYPE_ID AS STO_TYPE_ID,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_MOVEMENT_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MA_MOVEMENT_PRE_1")

# COMMAND ----------
# DBTITLE 1, UPD_MA_MOVEMENT_DAY_2


df_2=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        PO_NBR AS PO_NBR,
        PO_LINE_NBR AS PO_LINE_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        STO_TYPE_ID AS STO_TYPE_ID,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_MOVEMENT_PRE_1""")

df_2.createOrReplaceTempView("UPD_MA_MOVEMENT_DAY_2")

# COMMAND ----------
# DBTITLE 1, MA_MOVEMENT_DAY


spark.sql("""INSERT INTO MA_MOVEMENT_DAY SELECT DAY_DT AS DAY_DT,
PRODUCT_ID AS PRODUCT_ID,
LOCATION_ID AS LOCATION_ID,
MOVEMENT_ID AS MOVEMENT_ID,
PO_NBR AS PO_NBR,
PO_LINE_NBR AS PO_LINE_NBR,
MA_EVENT_ID AS MA_EVENT_ID,
STO_TYPE_ID AS STO_TYPE_ID,
MA_TRANS_AMT AS MA_TRANS_AMT,
MA_TRANS_COST AS MA_TRANS_COST,
MA_TRANS_QTY AS MA_TRANS_QTY,
null AS SALES_ADJ_AMT,
EXCH_RATE_PCT AS EXCH_RATE_PCT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_MA_MOVEMENT_DAY_2""")