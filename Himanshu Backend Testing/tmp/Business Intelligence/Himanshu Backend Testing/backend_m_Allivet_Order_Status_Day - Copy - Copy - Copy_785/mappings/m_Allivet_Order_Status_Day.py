# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_STATUS_DAY_0


df_0=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        RX_HOLD_DT AS RX_HOLD_DT,
        OPEN_DT AS OPEN_DT,
        COMPLETE_DT AS COMPLETE_DT,
        VOID_DT AS VOID_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_STATUS_DAY""")

df_0.createOrReplaceTempView("ALLIVET_ORDER_STATUS_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_ORDER_STATUS_DAY_1


df_1=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        RX_HOLD_DT AS RX_HOLD_DT,
        OPEN_DT AS OPEN_DT,
        COMPLETE_DT AS COMPLETE_DT,
        VOID_DT AS VOID_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_STATUS_DAY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_ORDER_STATUS_DAY_1")

# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY_2


df_2=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        PETSMART_ORDER_DT AS PETSMART_ORDER_DT,
        ORDER_STATUS AS ORDER_STATUS,
        PRODUCT_ID AS PRODUCT_ID,
        PETSMART_ORDER_NBR AS PETSMART_ORDER_NBR,
        PETSMART_SKU_NBR AS PETSMART_SKU_NBR,
        ALLIVET_SKU_NBR AS ALLIVET_SKU_NBR,
        SUB_TOTAL_AMT AS SUB_TOTAL_AMT,
        FREIGHT_COST AS FREIGHT_COST,
        TOTAL_AMT AS TOTAL_AMT,
        SHIP_METHOD_CD AS SHIP_METHOD_CD,
        ORDER_VOIDED_FLAG AS ORDER_VOIDED_FLAG,
        ORDER_ONHOLD_FLAG AS ORDER_ONHOLD_FLAG,
        ORDER_CREATED_DT AS ORDER_CREATED_DT,
        ORDER_MODIFIED_DT AS ORDER_MODIFIED_DT,
        SHIPPED_DT AS SHIPPED_DT,
        ORDER_SHIPPED_FLAG AS ORDER_SHIPPED_FLAG,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMT AS AUTOSHIP_DISCOUNT_AMT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        RISKORDER_FLAG AS RISKORDER_FLAG,
        RISK_REASON AS RISK_REASON,
        ORIG_SHIP_METHOD_CD AS ORIG_SHIP_METHOD_CD,
        SHIP_HOLD_FLAG AS SHIP_HOLD_FLAG,
        SHIP_HOLD_DT AS SHIP_HOLD_DT,
        SHIP_RELEASE_DT AS SHIP_RELEASE_DT,
        ORDER_QTY AS ORDER_QTY,
        ITEM_DESC AS ITEM_DESC,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DT AS ORDER_DETAIL_CREATED_DT,
        ORDER_DETAIL_MODIFIED_DT AS ORDER_DETAIL_MODIFIED_DT,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CD AS VET_CD,
        PET_CD AS PET_CD,
        ORDER_DETAIL_ONHOLD_FLAG AS ORDER_DETAIL_ONHOLD_FLAG,
        ONHOLD_TO_FILL_FLAG AS ONHOLD_TO_FILL_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_DAY""")

df_2.createOrReplaceTempView("ALLIVET_ORDER_DAY_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_ORDER_DAY_3


df_3=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_DAY_2 
    WHERE
        (
            UPDATE_TSTMP >= current_date 
            OR LOAD_TSTMP >= current_date
        )""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_ORDER_DAY_3")

# COMMAND ----------
# DBTITLE 1, ALLIVET_INVOICE_DAY_4


df_4=spark.sql("""
    SELECT
        INVOICE_POSTING_DT AS INVOICE_POSTING_DT,
        INVOICE_CD AS INVOICE_CD,
        TXN_TYPE AS TXN_TYPE,
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ALLIVET_SKU_NBR AS ALLIVET_SKU_NBR,
        PRODUCT_ID AS PRODUCT_ID,
        PETSMART_ORDER_NBR AS PETSMART_ORDER_NBR,
        PETSMART_SKU_NBR AS PETSMART_SKU_NBR,
        UPC_ID AS UPC_ID,
        SOLD_UNITS_QTY AS SOLD_UNITS_QTY,
        PRODUCT_COST AS PRODUCT_COST,
        RETAIL_PRICE AS RETAIL_PRICE,
        MANUFACTURER AS MANUFACTURER,
        BRAND_NAME AS BRAND_NAME,
        TITLE AS TITLE,
        FREIGHT_FEE_AMT AS FREIGHT_FEE_AMT,
        PACKAGING_FEE_AMT AS PACKAGING_FEE_AMT,
        DISPENSING_FEE_AMT AS DISPENSING_FEE_AMT,
        SHIPPED_DT AS SHIPPED_DT,
        FULFILLMENT_ORIGIN_ZIP_CD AS FULFILLMENT_ORIGIN_ZIP_CD,
        SHIP_CARRIER_NAME AS SHIP_CARRIER_NAME,
        TRACKING_NBR AS TRACKING_NBR,
        ALLIVET_CUSTOMER_NBR AS ALLIVET_CUSTOMER_NBR,
        PET_NAME AS PET_NAME,
        PET_TYPE AS PET_TYPE,
        BREED_TYPE AS BREED_TYPE,
        PET_GENDER AS PET_GENDER,
        PET_BIRT_DT AS PET_BIRT_DT,
        PET_WEIGHT AS PET_WEIGHT,
        PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
        PET_MEDICAL_CONDITION AS PET_MEDICAL_CONDITION,
        PET_PREGNANT_FLAG AS PET_PREGNANT_FLAG,
        VET_CLINIC_NAME AS VET_CLINIC_NAME,
        VET_NAME AS VET_NAME,
        VET_ADDRESS AS VET_ADDRESS,
        VET_CITY AS VET_CITY,
        VET_STATE_CD AS VET_STATE_CD,
        VET_ZIP_CD AS VET_ZIP_CD,
        VET_PHONE_NBR AS VET_PHONE_NBR,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_INVOICE_DAY""")

df_4.createOrReplaceTempView("ALLIVET_INVOICE_DAY_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_INVOICE_DAY_5


df_5=spark.sql("""
    SELECT
        INVOICE_POSTING_DT AS INVOICE_POSTING_DT,
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_INVOICE_DAY_4 
    WHERE
        (
            UPDATE_TSTMP >= current_date 
            OR LOAD_TSTMP >= current_date
        ) 
        AND lower(TXN_TYPE) = 'invoice'""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_INVOICE_DAY_5")

# COMMAND ----------
# DBTITLE 1, ALLIVET_SYSTEM_AUDIT_DAY_6


df_6=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        ALLIVET_ORDER_CNTR AS ALLIVET_ORDER_CNTR,
        ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
        OPERATION AS OPERATION,
        ALLIVET_CUSTOMER_NBR AS ALLIVET_CUSTOMER_NBR,
        EXTRA_INFO AS EXTRA_INFO,
        ORDER_CHG_DT AS ORDER_CHG_DT,
        FROM_ITEM_CD AS FROM_ITEM_CD,
        TO_ITEM_CD AS TO_ITEM_CD,
        PRESCRIPTION_ID AS PRESCRIPTION_ID,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_SYSTEM_AUDIT_DAY""")

df_6.createOrReplaceTempView("ALLIVET_SYSTEM_AUDIT_DAY_6")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_7


df_7=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        OPERATION AS OPERATION,
        ORDER_CHG_DT AS ORDER_CHG_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_SYSTEM_AUDIT_DAY_6 
    WHERE
        (
            UPDATE_TSTMP >= current_date 
            OR LOAD_TSTMP >= current_date
        )""")

df_7.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_7")

# COMMAND ----------
# DBTITLE 1, JNRTRANS_8


df_8=spark.sql("""
    SELECT
        DETAIL.ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        DETAIL.OPERATION AS OPERATION,
        DETAIL.ORDER_CHG_DT AS ORDER_CHG_DT,
        MASTER.ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR1,
        MASTER.ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ALLIVET_ORDER_DAY_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_7 DETAIL 
            ON MASTER.ALLIVET_ORDER_NBR = DETAIL.ALLIVET_ORDER_NBR""")

df_8.createOrReplaceTempView("JNRTRANS_8")

# COMMAND ----------
# DBTITLE 1, EXPTRANS1_9


df_9=spark.sql("""
    SELECT
        OPERATION AS OPERATION,
        ORDER_CHG_DT AS ORDER_CHG_DT,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        IFF(ISNULL(ALLIVET_ORDER_NBR),
        ALLIVET_ORDER_NBR1,
        ALLIVET_ORDER_NBR) AS ALLIVET_ORDER_NBR_New,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNRTRANS_8""")

df_9.createOrReplaceTempView("EXPTRANS1_9")

# COMMAND ----------
# DBTITLE 1, JNRTRANS1_10


df_10=spark.sql("""
    SELECT
        DETAIL.ALLIVET_ORDER_NBR_New AS ALLIVET_ORDER_NBR_New,
        DETAIL.OPERATION AS OPERATION,
        DETAIL.ORDER_CHG_DT AS ORDER_CHG_DT,
        DETAIL.ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        MASTER.INVOICE_POSTING_DT AS INVOICE_POSTING_DT,
        MASTER.ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ALLIVET_INVOICE_DAY_5 MASTER 
    LEFT JOIN
        EXPTRANS1_9 DETAIL 
            ON MASTER.ALLIVET_ORDER_NBR = DETAIL.ALLIVET_ORDER_NBR_New""")

df_10.createOrReplaceTempView("JNRTRANS1_10")

# COMMAND ----------
# DBTITLE 1, Exp_Dates_11


df_11=spark.sql("""
    SELECT
        IFF(ISNULL(ALLIVET_ORDER_NBR_New),
        ALLIVET_ORDER_NBR_New,
        ALLIVET_ORDER_NBR_New) AS ALLIVET_ORDER_NBR_New,
        ORDER_CHG_DT AS ORDER_CHG_DT,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        IFF((INSTR(LTRIM(RTRIM(UPPER(OPERATION))),
        UPPER('ReSentToRPH')) > 0 
        OR INSTR(LTRIM(RTRIM(UPPER(OPERATION))),
        UPPER('ReHold')) > 0 
        OR LTRIM(RTRIM(OPERATION)) = ''),
        ORDER_CHG_DT,
        NULL) AS RX_HOLD_DT,
        IFF((INSTR(LTRIM(RTRIM(UPPER(OPERATION))),
        UPPER('PrintRxAndShippingLabel')) > 0 
        OR INSTR(LTRIM(RTRIM(UPPER(OPERATION))),
        UPPER('VoidInvoice')) > 0),
        ORDER_CHG_DT,
        NULL) AS OPEN_DT,
        INVOICE_POSTING_DT AS COMPLETE_DT,
        IFF((INSTR(LTRIM(RTRIM(UPPER(OPERATION))),
        UPPER('VoidSO')) > 0),
        ORDER_CHG_DT,
        NULL) AS VOID_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNRTRANS1_10""")

df_11.createOrReplaceTempView("Exp_Dates_11")

# COMMAND ----------
# DBTITLE 1, Srt_CHG_DT_12


df_12=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR_New AS ALLIVET_ORDER_NBR,
        ORDER_CHG_DT AS ORDER_CHG_DT,
        RX_HOLD_DT AS RX_HOLD_DT,
        OPEN_DT AS OPEN_DT,
        COMPLETE_DT AS COMPLETE_DT,
        VOID_DT AS VOID_DT,
        ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_Dates_11 
    ORDER BY
        ALLIVET_ORDER_NBR ASC,
        ORDER_CHG_DT ASC""")

df_12.createOrReplaceTempView("Srt_CHG_DT_12")

# COMMAND ----------
# DBTITLE 1, Agg_Order_OrderLN_13


df_13=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        MAX(RX_HOLD_DT) AS o_RX_HOLD_DT,
        MAX(OPEN_DT) AS o_OPEN_DT,
        MAX(COMPLETE_DT) AS o_COMPLETE_DT,
        MAX(VOID_DT) AS o_VOID_DT,
        MIN(ORDER_CHG_DT) AS o_ORDER_CHG_DT,
        min(ALLIVET_ORDER_DT) AS o_ALLIVET_ORDER_DT 
    FROM
        Srt_CHG_DT_12 
    GROUP BY
        ALLIVET_ORDER_NBR""")

df_13.createOrReplaceTempView("Agg_Order_OrderLN_13")

# COMMAND ----------
# DBTITLE 1, Jnr_Order_Status_14


df_14=spark.sql("""
    SELECT
        DETAIL.ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        DETAIL.o_RX_HOLD_DT AS o_RX_HOLD_DT,
        DETAIL.o_OPEN_DT AS o_OPEN_DT,
        DETAIL.o_COMPLETE_DT AS o_COMPLETE_DT,
        DETAIL.o_VOID_DT AS o_VOID_DT,
        DETAIL.o_ORDER_CHG_DT AS o_ORDER_CHG_DT,
        DETAIL.o_ALLIVET_ORDER_DT AS o_ALLIVET_ORDER_DT,
        MASTER.ALLIVET_ORDER_NBR AS LKP_ALLIVET_ORDER_NBR,
        MASTER.RX_HOLD_DT AS LKP_RX_HOLD_DT,
        MASTER.OPEN_DT AS LKP_OPEN_DT,
        MASTER.COMPLETE_DT AS LKP_COMPLETE_DT,
        MASTER.VOID_DT AS LKP_VOID_DT,
        MASTER.LOAD_TSTMP AS Lkp_LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ALLIVET_ORDER_STATUS_DAY_1 MASTER 
    LEFT JOIN
        Agg_Order_OrderLN_13 DETAIL 
            ON MASTER.ALLIVET_ORDER_NBR = DETAIL.ALLIVET_ORDER_NBR""")

df_14.createOrReplaceTempView("Jnr_Order_Status_14")

# COMMAND ----------
# DBTITLE 1, Exp_Update_Flag_15


df_15=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        o_ORDER_CHG_DT AS o_ORDER_CHG_DT,
        o_ALLIVET_ORDER_DT AS o_ALLIVET_ORDER_DT,
        IFF(ISNULL(IFF((ISNULL(o_RX_HOLD_DT) 
        AND NOT isnull(o_ALLIVET_ORDER_DT)),
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT)),
        LKP_RX_HOLD_DT,
        IFF((ISNULL(o_RX_HOLD_DT) 
        AND NOT isnull(o_ALLIVET_ORDER_DT)),
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT)) AS o_RX_HOLD_DT,
        IFF(ISNULL(o_OPEN_DT),
        LKP_OPEN_DT,
        o_OPEN_DT) AS o_OPEN_DT,
        IFF(ISNULL(o_COMPLETE_DT),
        LKP_COMPLETE_DT,
        o_COMPLETE_DT) AS o_COMPLETE_DT,
        IFF(ISNULL(o_VOID_DT),
        LKP_VOID_DT,
        o_VOID_DT) AS o_VOID_DT,
        IFF(ISNULL(Lkp_LOAD_TSTMP),
        current_timestamp,
        Lkp_LOAD_TSTMP) AS LOAD_TSTMP,
        current_timestamp AS UPDATE_TSTMP,
        IFF((ISNULL(LKP_ALLIVET_ORDER_NBR)),
        1,
        IFF(NOT (ISNULL(LKP_ALLIVET_ORDER_NBR)) 
        AND ((IFF(ISNULL(IFF(ISNULL(IFF((ISNULL(o_RX_HOLD_DT) 
        AND NOT isnull(o_ALLIVET_ORDER_DT)),
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT)),
        LKP_RX_HOLD_DT,
        IFF((ISNULL(o_RX_HOLD_DT) 
        AND NOT isnull(o_ALLIVET_ORDER_DT)),
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT))),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        IFF((ISNULL(o_RX_HOLD_DT) 
        AND NOT isnull(o_ALLIVET_ORDER_DT)),
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT)) <> IFF(ISNULL(LKP_RX_HOLD_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        LKP_RX_HOLD_DT)) 
        OR IFF(ISNULL(o_OPEN_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        o_OPEN_DT) <> IFF(ISNULL(LKP_OPEN_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        LKP_OPEN_DT) 
        OR IFF(ISNULL(o_VOID_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        o_VOID_DT) <> IFF(ISNULL(LKP_VOID_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        LKP_VOID_DT) 
        OR IFF(ISNULL(o_COMPLETE_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        o_COMPLETE_DT) <> IFF(ISNULL(LKP_COMPLETE_DT),
        TO_DATE('9999-12-31',
        'YYYY-MM-DD'),
        LKP_COMPLETE_DT)),
        2,
        0)) AS UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_Order_Status_14""")

df_15.createOrReplaceTempView("Exp_Update_Flag_15")

# COMMAND ----------
# DBTITLE 1, Ftr_Insert_Update_16


df_16=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        o_RX_HOLD_DT AS o_RX_HOLD_DT,
        o_OPEN_DT AS o_OPEN_DT,
        o_COMPLETE_DT AS o_COMPLETE_DT,
        o_VOID_DT AS o_VOID_DT,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        UPDATE_FLAG AS UPDATE_FLAG,
        o_ORDER_CHG_DT AS o_ORDER_CHG_DT,
        o_ALLIVET_ORDER_DT AS o_ALLIVET_ORDER_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_Update_Flag_15 
    WHERE
        UPDATE_FLAG IN (
            1, 2
        )""")

df_16.createOrReplaceTempView("Ftr_Insert_Update_16")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_17


df_17=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        o_OPEN_DT AS o_OPEN_DT,
        o_COMPLETE_DT AS o_COMPLETE_DT,
        o_VOID_DT AS o_VOID_DT,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        UPDATE_FLAG AS UPDATE_FLAG,
        o_ALLIVET_ORDER_DT AS o_ALLIVET_ORDER_DT,
        IFF(UPDATE_FLAG = 1,
        o_ALLIVET_ORDER_DT,
        o_RX_HOLD_DT) AS o_RX_HOLD_DT1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Ftr_Insert_Update_16""")

df_17.createOrReplaceTempView("EXPTRANS_17")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_18


df_18=spark.sql("""
    SELECT
        ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
        o_RX_HOLD_DT1 AS o_RX_HOLD_DT,
        o_OPEN_DT AS o_OPEN_DT,
        o_COMPLETE_DT AS o_COMPLETE_DT,
        o_VOID_DT AS o_VOID_DT,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        UPDATE_FLAG AS UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_17""")

df_18.createOrReplaceTempView("UPDTRANS_18")

# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_STATUS_DAY


spark.sql("""INSERT INTO ALLIVET_ORDER_STATUS_DAY SELECT ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
o_RX_HOLD_DT AS RX_HOLD_DT,
o_OPEN_DT AS OPEN_DT,
o_COMPLETE_DT AS COMPLETE_DT,
o_VOID_DT AS VOID_DT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPDTRANS_18""")