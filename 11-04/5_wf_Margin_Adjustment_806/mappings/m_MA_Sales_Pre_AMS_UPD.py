# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SALES_TRANS_UPC_OFFER_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        OFFER_ID AS OFFER_ID,
        SCAN_SEQ_NBR AS SCAN_SEQ_NBR,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        OFFER_AMT AS OFFER_AMT,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_UPC_OFFER""")

df_0.createOrReplaceTempView("SALES_TRANS_UPC_OFFER_0")

# COMMAND ----------
# DBTITLE 1, MA_SALES_PRE_1


df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        MA_SALES_AMT AS MA_SALES_AMT,
        MA_SALES_QTY AS MA_SALES_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        RESTATE_FLAG AS RESTATE_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_SALES_PRE""")

df_1.createOrReplaceTempView("MA_SALES_PRE_1")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_2


df_2=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        COMPANY_ID AS COMPANY_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT""")

df_2.createOrReplaceTempView("MA_EVENT_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_SALES_PRE_3


df_3=spark.sql("""
    SELECT
        DAY_DT,
        LOCATION_ID,
        SALES_INSTANCE_ID,
        UPC_ID,
        TP_INVOICE_NBR,
        PARENT_UPC_ID,
        COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR,
        MA_EVENT_ID,
        CASE 
            WHEN MA_SALES_AMT < 0 THEN (ABS(MA_SALES_AMT) + MA_AMT_DIFF) * -1 
            ELSE MA_SALES_AMT + MA_AMT_DIFF 
        END AS NEW_MA_SALES_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            T1.*,
            ROW_NUMBER() OVER (PARTITION 
        BY
            SALES_INSTANCE_ID,
            OFFER_ID 
        ORDER BY
            (ABS(OFFER_AMT) - ABS(MA_SALES_AMT)) DESC) AS RNK 
        FROM
            (SELECT
                MS.DAY_DT,
                MS.LOCATION_ID,
                MS.SALES_INSTANCE_ID,
                MS.UPC_ID,
                MS.TP_INVOICE_NBR,
                MS.PARENT_UPC_ID,
                MS.COMBO_TYPE_CD,
                MS.POS_TXN_SEQ_NBR,
                MS.MA_EVENT_ID,
                MS.MA_SALES_AMT,
                ME.OFFER_ID,
                SO.OFFER_AMT,
                ((CASE 
                    WHEN MA_PCT_IND = 1 THEN ROUND(SUM(ABS(SO.OFFER_AMT)) OVER (PARTITION 
                BY
                    SO.SALES_INSTANCE_ID,
                    SO.OFFER_ID ) * (ME.MA_AMT / 100),
                    2) 
                    ELSE ME.MA_AMT * SO.OFFER_CNT 
                END) - SUM(ABS(MS.MA_SALES_AMT)) OVER (PARTITION 
            BY
                MS.SALES_INSTANCE_ID,
                ME.OFFER_ID )) AS MA_AMT_DIFF 
            FROM
                MA_SALES_PRE MS,
                MA_EVENT ME,
                (SELECT
                    DISTINCT DAY_DT,
                    SALES_INSTANCE_ID,
                    OFFER_ID,
                    UPC_ID,
                    POS_TXN_SEQ_NBR,
                    COUNT(DISTINCT SCAN_SEQ_NBR) OVER (PARTITION 
                BY
                    DAY_DT,
                    SALES_INSTANCE_ID,
                    OFFER_ID ) AS OFFER_CNT,
                    SUM(OFFER_AMT) OVER (PARTITION 
                BY
                    DAY_DT,
                    SALES_INSTANCE_ID,
                    OFFER_ID,
                    UPC_ID,
                    POS_TXN_SEQ_NBR ) AS OFFER_AMT 
                FROM
                    SALES_TRANS_UPC_OFFER 
                WHERE
                    DAY_DT >= (
                        SELECT
                            MIN(DAY_DT) 
                        FROM
                            MA_SALES_PRE
                    )
                ) SO 
            WHERE
                MS.MA_EVENT_ID = ME.MA_EVENT_ID 
                AND MS.DAY_DT = SO.DAY_DT 
                AND MS.SALES_INSTANCE_ID = SO.SALES_INSTANCE_ID 
                AND MS.UPC_ID = SO.UPC_ID 
                AND MS.POS_TXN_SEQ_NBR = SO.POS_TXN_SEQ_NBR 
                AND ME.OFFER_ID = SO.OFFER_ID 
                AND ME.MA_EVENT_TYPE_ID = 2 
                AND ME.MA_EVENT_SOURCE_ID = 6) T1 
        WHERE
            MA_AMT_DIFF <> 0 
            AND ABS(OFFER_AMT) - ABS(MA_SALES_AMT) >= MA_AMT_DIFF
        ) T2 
    WHERE
        RNK = 1""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_MA_SALES_PRE_3")

# COMMAND ----------
# DBTITLE 1, UPD_UpdateOnly_4


df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        MA_SALES_AMT AS MA_SALES_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_SALES_PRE_3""")

df_4.createOrReplaceTempView("UPD_UpdateOnly_4")

# COMMAND ----------
# DBTITLE 1, MA_SALES_PRE


spark.sql("""INSERT INTO MA_SALES_PRE SELECT DAY_DT AS DAY_DT,
DAY_DT AS DAY_DT,
LOCATION_ID AS LOCATION_ID,
SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
UPC_ID AS UPC_ID,
TP_INVOICE_NBR AS TP_INVOICE_NBR,
PARENT_UPC_ID AS PARENT_UPC_ID,
COMBO_TYPE_CD AS COMBO_TYPE_CD,
POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
MA_EVENT_ID AS MA_EVENT_ID,
null AS PRODUCT_ID,
null AS SALES_CUST_CAPTURE_CD,
MA_SALES_AMT AS MA_SALES_AMT,
null AS MA_SALES_QTY,
null AS EXCH_RATE_PCT,
null AS RESTATE_FLAG FROM UPD_UpdateOnly_4""")