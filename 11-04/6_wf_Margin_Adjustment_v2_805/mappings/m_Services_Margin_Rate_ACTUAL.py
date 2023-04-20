# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_RATE_PRE_0


df_0=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_RATE_PRE""")

df_0.createOrReplaceTempView("SERVICES_MARGIN_RATE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SERVICES_MARGIN_RATE_PRE_1


df_1=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_RATE_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SERVICES_MARGIN_RATE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_RATE_2


df_2=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_RATE""")

df_2.createOrReplaceTempView("SERVICES_MARGIN_RATE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SERVICES_MARGIN_RATE_3


df_3=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_RATE_2 
    WHERE
        SERVICES_MARGIN_RATE.WEEK_DT IN (
            SELECT
                DISTINCT WEEK_DT 
            FROM
                SERVICES_MARGIN_RATE_PRE
        )""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SERVICES_MARGIN_RATE_3")

# COMMAND ----------
# DBTITLE 1, JNR_SERVICES_MARGIN_4


df_4=spark.sql("""
    SELECT
        MASTER.WEEK_DT AS WEEK_DT,
        MASTER.LOCATION_ID AS LOCATION_ID,
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID,
        MASTER.MARGIN_RATE AS MARGIN_RATE,
        DETAIL.WEEK_DT AS WEEK_DT_OLD,
        DETAIL.LOCATION_ID AS LOCATION_ID_OLD,
        DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID_OLD,
        DETAIL.MARGIN_RATE AS MARGIN_RATE_OLD,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP_OLD,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_PRE_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_3 DETAIL 
            ON MASTER.WEEK_DT = WEEK_DT_OLD 
            AND LOCATION_ID = LOCATION_ID_OLD 
            AND SAP_DEPT_ID = DETAIL.SAP_DEPT_ID""")

df_4.createOrReplaceTempView("JNR_SERVICES_MARGIN_4")

# COMMAND ----------
# DBTITLE 1, EXP_MARGIN_RATE_5


df_5=spark.sql("""
    SELECT
        IFF(ISNULL(WEEK_DT),
        WEEK_DT_OLD,
        WEEK_DT) AS WEEK_DT_OUT,
        IFF(ISNULL(LOCATION_ID),
        LOCATION_ID_OLD,
        LOCATION_ID) AS LOCATION_ID_OUT,
        IFF(ISNULL(SAP_DEPT_ID),
        SAP_DEPT_ID_OLD,
        SAP_DEPT_ID) AS SAP_DEPT_ID_OUT,
        IFF(ISNULL(MARGIN_RATE),
        0,
        MARGIN_RATE) AS MARGIN_RATE_OUT,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP_OLD),
        current_timestamp,
        LOAD_TSTMP_OLD) AS LOAD_TSTMP,
        IFF(ISNULL(WEEK_DT_OLD),
        DD_INSERT,
        IFF(IFF(ISNULL(MARGIN_RATE_OLD),
        -1,
        MARGIN_RATE_OLD) <> IFF(ISNULL(MARGIN_RATE),
        -1,
        MARGIN_RATE),
        DD_UPDATE,
        DD_REJECT)) AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_SERVICES_MARGIN_4""")

df_5.createOrReplaceTempView("EXP_MARGIN_RATE_5")

# COMMAND ----------
# DBTITLE 1, FIL_REJECTED_6


df_6=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_MARGIN_RATE_5 
    WHERE
        UPDATE_STRATEGY <> DD_REJECT""")

df_6.createOrReplaceTempView("FIL_REJECTED_6")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_7


df_7=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_REJECTED_6""")

df_7.createOrReplaceTempView("UPD_STRATEGY_7")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_RATE


spark.sql("""INSERT INTO SERVICES_MARGIN_RATE SELECT WEEK_DT AS WEEK_DT,
WEEK_DT AS WEEK_DT,
LOCATION_ID AS LOCATION_ID,
LOCATION_ID AS LOCATION_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
MARGIN_RATE AS MARGIN_RATE,
MARGIN_RATE AS MARGIN_RATE,
UPDATE_TSTMP AS UPDATE_TSTMP,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_STRATEGY_7""")