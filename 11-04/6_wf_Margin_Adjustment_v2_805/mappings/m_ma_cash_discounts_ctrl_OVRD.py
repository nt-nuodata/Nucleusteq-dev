# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, USR_MA_CASH_DISCOUNT_OVRD_CTRL_0


df_0=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_MA_CASH_DISCOUNT_OVRD_CTRL""")

df_0.createOrReplaceTempView("USR_MA_CASH_DISCOUNT_OVRD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_MA_CASH_DISCOUNT_OVRD_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_MA_CASH_DISCOUNT_OVRD_CTRL_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_MA_CASH_DISCOUNT_OVRD_CTRL_1")

# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL_2


df_2=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL""")

df_2.createOrReplaceTempView("MA_CASH_DISCOUNT_CTRL_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_3


df_3=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_3")

# COMMAND ----------
# DBTITLE 1, JNR_MA_CASH_DISCOUNT_OVRD_4


df_4=spark.sql("""
    SELECT
        MASTER.FISCAL_MO AS FISCAL_MO1,
        MASTER.SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID1,
        MASTER.OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT1,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        DETAIL.OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_USR_MA_CASH_DISCOUNT_OVRD_CTRL_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_3 DETAIL 
            ON MASTER.FISCAL_MO = FISCAL_MO 
            AND SOURCE_VENDOR_ID1 = DETAIL.SOURCE_VENDOR_ID""")

df_4.createOrReplaceTempView("JNR_MA_CASH_DISCOUNT_OVRD_4")

# COMMAND ----------
# DBTITLE 1, EXP_OVRD_CASH_DISCOUNT_PCT_5


df_5=spark.sql("""
    SELECT
        OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT1,
        IFF(ISNULL(FISCAL_MO1),
        FISCAL_MO,
        FISCAL_MO1) AS FISCAL_MO,
        IFF(ISNULL(SOURCE_VENDOR_ID1),
        SOURCE_VENDOR_ID,
        SOURCE_VENDOR_ID1) AS SOURCE_VENDOR_ID,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(FISCAL_MO),
        'I',
        IFF(IFF(ISNULL(OVRD_CASH_DISCOUNT_PCT1),
        -99,
        OVRD_CASH_DISCOUNT_PCT1) <> IFF(ISNULL(OVRD_CASH_DISCOUNT_PCT),
        -99,
        OVRD_CASH_DISCOUNT_PCT),
        'U',
        'X')) AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MA_CASH_DISCOUNT_OVRD_4""")

df_5.createOrReplaceTempView("EXP_OVRD_CASH_DISCOUNT_PCT_5")

# COMMAND ----------
# DBTITLE 1, FIL_INS_UPD_FLAG_6


df_6=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT1,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_OVRD_CASH_DISCOUNT_PCT_5 
    WHERE
        INS_UPD_FLAG <> 'X'""")

df_6.createOrReplaceTempView("FIL_INS_UPD_FLAG_6")

# COMMAND ----------
# DBTITLE 1, SRT_DISTINCT_7


df_7=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT1,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_INS_UPD_FLAG_6 
    ORDER BY
        FISCAL_MO ASC,
        SOURCE_VENDOR_ID ASC,
        OVRD_CASH_DISCOUNT_PCT1 ASC,
        UPDATE_TSTMP ASC,
        LOAD_TSTMP ASC,
        INS_UPD_FLAG ASC""")

df_7.createOrReplaceTempView("SRT_DISTINCT_7")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_8


df_8=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT1,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SRT_DISTINCT_7""")

df_8.createOrReplaceTempView("UPD_STRATEGY_8")

# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL


spark.sql("""INSERT INTO MA_CASH_DISCOUNT_CTRL SELECT FISCAL_MO AS FISCAL_MO,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
null AS EST_CASH_DISCOUNT_PCT,
null AS ACT_NET_SALES_COST,
null AS ACT_CASH_DISCOUNT_GL_AMT,
null AS ACT_CASH_DISCOUNT_PCT,
OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_STRATEGY_8""")