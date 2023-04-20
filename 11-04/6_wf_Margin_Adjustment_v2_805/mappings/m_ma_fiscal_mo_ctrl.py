# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_FISCAL_MO_CTRL_0


df_0=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        RESTATE_DT AS RESTATE_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_FISCAL_MO_CTRL""")

df_0.createOrReplaceTempView("MA_FISCAL_MO_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_FISCAL_MO_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_FISCAL_MO_CTRL_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MA_FISCAL_MO_CTRL_1")

# COMMAND ----------
# DBTITLE 1, DAYS_2


df_2=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_2.createOrReplaceTempView("DAYS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_DAYS_3


df_3=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DAYS_2 
    WHERE
        DAYS.DAY_DT = CURRENT_DATE 
        AND DAYS.FISCAL_DAY_OF_MO_NBR = 7""")

df_3.createOrReplaceTempView("SQ_Shortcut_To_DAYS_3")

# COMMAND ----------
# DBTITLE 1, EXP_FISCAL_MO_4


df_4=spark.sql("""
    SELECT
        DAY_DT AS RESTATE_DT,
        IFF(FISCAL_MO_NBR = 1,
        FISCAL_MO - 89,
        FISCAL_MO - 1) AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_DAYS_3""")

df_4.createOrReplaceTempView("EXP_FISCAL_MO_4")

# COMMAND ----------
# DBTITLE 1, JNR_FISCAL_MO_5


df_5=spark.sql("""
    SELECT
        DETAIL.RESTATE_DT AS RESTATE_DT,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        MASTER.FISCAL_MO AS FISCAL_MO1,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_FISCAL_MO_CTRL_1 MASTER 
    LEFT JOIN
        EXP_FISCAL_MO_4 DETAIL 
            ON MASTER.FISCAL_MO = DETAIL.FISCAL_MO""")

df_5.createOrReplaceTempView("JNR_FISCAL_MO_5")

# COMMAND ----------
# DBTITLE 1, EXP_INS_UPD_FLAG_6


df_6=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        RESTATE_DT AS RESTATE_DT,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(FISCAL_MO1),
        'I',
        'U') AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_FISCAL_MO_5""")

df_6.createOrReplaceTempView("EXP_INS_UPD_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_7


df_7=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        RESTATE_DT AS RESTATE_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_INS_UPD_FLAG_6""")

df_7.createOrReplaceTempView("UPD_STRATEGY_7")

# COMMAND ----------
# DBTITLE 1, MA_FISCAL_MO_CTRL


spark.sql("""INSERT INTO MA_FISCAL_MO_CTRL SELECT FISCAL_MO AS FISCAL_MO,
RESTATE_DT AS RESTATE_DT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_STRATEGY_7""")