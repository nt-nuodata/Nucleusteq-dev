# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SEQ_MA_EVENT_ID


spark.sql("""CREATE TABLE SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);""")

spark.sql("""INSERT INTO SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(2, 1, 1)""")

# COMMAND ----------
# DBTITLE 1, MaRoyaltySKU_1


df_1=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        ProductId AS ProductId,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MaRoyaltySKU""")

df_1.createOrReplaceTempView("MaRoyaltySKU_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MaRoyaltySKU_2


df_2=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        ProductId AS ProductId,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MaRoyaltySKU_1""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_MaRoyaltySKU_2")

# COMMAND ----------
# DBTITLE 1, MaRoyaltyBrand_3


df_3=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        BrandCd AS BrandCd,
        SkuListInd AS SkuListInd,
        ContractStartDate AS ContractStartDate,
        ContractEndDate AS ContractEndDate,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyPct AS RoyaltyPct,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MaRoyaltyBrand""")

df_3.createOrReplaceTempView("MaRoyaltyBrand_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MaRoyaltyBrand_4


df_4=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        BrandCd AS BrandCd,
        SkuListInd AS SkuListInd,
        ContractStartDate AS ContractStartDate,
        ContractEndDate AS ContractEndDate,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyPct AS RoyaltyPct,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MaRoyaltyBrand_3""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_MaRoyaltyBrand_4")

# COMMAND ----------
# DBTITLE 1, JNR_RoyaltyBrandIdNew_5


df_5=spark.sql("""
    SELECT
        MASTER.RoyaltyBrandId AS RoyaltyBrandId1,
        MASTER.RoyaltyBrandDesc AS RoyaltyBrandDesc,
        MASTER.BrandCd AS BrandCd,
        MASTER.SkuListInd AS SkuListInd,
        MASTER.ContractStartDate AS ContractStartDate,
        MASTER.ContractEndDate AS ContractEndDate,
        MASTER.RoyaltyFormulaCd AS RoyaltyFormulaCd,
        MASTER.RoyaltyPct AS RoyaltyPct,
        DETAIL.RoyaltyBrandId AS RoyaltyBrandId,
        DETAIL.ProductId AS ProductId,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MaRoyaltyBrand_4 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_MaRoyaltySKU_2 DETAIL 
            ON MASTER.RoyaltyBrandId = DETAIL.RoyaltyBrandId""")

df_5.createOrReplaceTempView("JNR_RoyaltyBrandIdNew_5")

# COMMAND ----------
# DBTITLE 1, EXP_RoyaltyBrandNew_6


df_6=spark.sql("""SELECT RoyaltyBrandId1 AS RoyaltyBrandId,
ProductId AS ProductId,
BrandCd AS BrandCd,
RoyaltyFormulaCd AS RoyaltyFormulaCd,
RoyaltyBrandDesc AS RoyaltyBrandDesc,
(RoyaltyPct * 100) * -1 AS RoyaltyPct1,
ContractStartDate AS ContractStartDate,
ContractEndDate AS ContractEndDate,
SkuListInd AS SkuListInd,
1 AS Flag,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM JNR_RoyaltyBrandIdNew_5""")

df_6.createOrReplaceTempView("EXP_RoyaltyBrandNew_6")

# COMMAND ----------
# DBTITLE 1, EXT_DAYS_7


df_7=spark.sql("""
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
        EXT_DAYS""")

df_7.createOrReplaceTempView("EXT_DAYS_7")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_EXT_DAYS_8


df_8=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        FISCAL_MO AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXT_DAYS_7""")

df_8.createOrReplaceTempView("SQ_Shortcut_to_EXT_DAYS_8")

# COMMAND ----------
# DBTITLE 1, EXP_Ext_Days_9


df_9=spark.sql("""SELECT DAY_DT AS DAY_DT,
FISCAL_MO AS FISCAL_MO,
1 AS Flag,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM SQ_Shortcut_to_EXT_DAYS_8""")

df_9.createOrReplaceTempView("EXP_Ext_Days_9")

# COMMAND ----------
# DBTITLE 1, JNR_MaRoyalty_10


df_10=spark.sql("""
    SELECT
        MASTER.RoyaltyBrandId AS RoyaltyBrandId,
        MASTER.ProductId AS ProductId,
        MASTER.BrandCd AS BrandCd,
        MASTER.RoyaltyFormulaCd AS RoyaltyFormulaCd,
        MASTER.RoyaltyBrandDesc AS RoyaltyBrandDesc,
        MASTER.RoyaltyPct1 AS RoyaltyPct1,
        MASTER.ContractStartDate AS ContractStartDate,
        MASTER.ContractEndDate AS ContractEndDate,
        MASTER.SkuListInd AS SkuListInd,
        MASTER.Flag AS Flag1,
        DETAIL.DAY_DT AS DAY_DT,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.Flag AS Flag,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_RoyaltyBrandNew_6 MASTER 
    LEFT JOIN
        EXP_Ext_Days_9 DETAIL 
            ON MASTER.Flag = DETAIL.Flag""")

df_10.createOrReplaceTempView("JNR_MaRoyalty_10")

# COMMAND ----------
# DBTITLE 1, FIL_Date_11


df_11=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        ProductId AS ProductId,
        BrandCd AS BrandCd,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        RoyaltyPct1 AS RoyaltyPct1,
        ContractStartDate AS ContractStartDate,
        ContractEndDate AS ContractEndDate,
        SkuListInd AS SkuListInd,
        DAY_DT AS DAY_DT,
        FISCAL_MO AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MaRoyalty_10 
    WHERE
        DAY_DT >= ContractStartDate 
        AND DAY_DT <= ContractEndDate 
        AND FISCAL_MO >= 201605 
        AND FISCAL_MO <= 299999""")

df_11.createOrReplaceTempView("FIL_Date_11")

# COMMAND ----------
# DBTITLE 1, AGG_Day_Dt_12


df_12=spark.sql("""
    SELECT
        RoyaltyBrandId AS RoyaltyBrandId,
        MIN(DAY_DT) AS START_DT,
        MAX(DAY_DT) AS END_DT,
        ProductId AS ProductId,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        BrandCd AS BrandCd,
        SkuListInd AS SkuListInd,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyPct1 AS RoyaltyPct1,
        FISCAL_MO AS FISCAL_MO 
    FROM
        FIL_Date_11 
    GROUP BY
        RoyaltyBrandId,
        ProductId,
        RoyaltyBrandDesc,
        BrandCd,
        SkuListInd,
        RoyaltyFormulaCd,
        RoyaltyPct1,
        FISCAL_MO""")

df_12.createOrReplaceTempView("AGG_Day_Dt_12")

# COMMAND ----------
# DBTITLE 1, EXP_PriductIdNew_13


df_13=spark.sql("""SELECT RoyaltyBrandId AS RoyaltyBrandId,
IIF(ISNULL(ProductId), 0, ProductId)  AS PRODUCT_ID_JOIN,
ProductId AS PRODUCT_ID,
START_DT AS START_DT,
END_DT AS END_DT,
RoyaltyBrandDesc AS RoyaltyBrandDesc,
BrandCd AS BrandCd,
SkuListInd AS SkuListInd,
RoyaltyFormulaCd AS RoyaltyFormulaCd,
RoyaltyPct1 AS RoyaltyPct1,
FISCAL_MO AS FISCAL_MO,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM AGG_Day_Dt_12""")

df_13.createOrReplaceTempView("EXP_PriductIdNew_13")

# COMMAND ----------
# DBTITLE 1, LKP_EXT_DAYS_14


df_14=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        EXP_PriductIdNew_13.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            DATEADD(day,
            1,
            WEEK_DT) AS DAY_DT 
        FROM
            EXT_DAYS 
        WHERE
            DAY_DT = DATEADD(dd, DATEDIFF(dd, 96, getdate()), 0)) AS EXT_DAYS 
    RIGHT OUTER JOIN
        EXP_PriductIdNew_13 
            ON DAY_DT < EXP_PriductIdNew_13.START_DT""")

df_14.createOrReplaceTempView("LKP_EXT_DAYS_14")

# COMMAND ----------
# DBTITLE 1, FIL_Having_15


df_15=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        RoyaltyBrandId AS RoyaltyBrandId,
        START_DT AS START_DT,
        END_DT AS END_DT,
        PRODUCT_ID_JOIN AS PRODUCT_ID_JOIN,
        PRODUCT_ID AS PRODUCT_ID,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        BrandCd AS BrandCd,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyPct1 AS RoyaltyPct1,
        FISCAL_MO AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_PriductIdNew_13 
    WHERE
        NOT ISNULL(DAY_DT)""")

df_15.createOrReplaceTempView("FIL_Having_15")

df_15=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        RoyaltyBrandId AS RoyaltyBrandId,
        START_DT AS START_DT,
        END_DT AS END_DT,
        PRODUCT_ID_JOIN AS PRODUCT_ID_JOIN,
        PRODUCT_ID AS PRODUCT_ID,
        RoyaltyBrandDesc AS RoyaltyBrandDesc,
        BrandCd AS BrandCd,
        RoyaltyFormulaCd AS RoyaltyFormulaCd,
        RoyaltyPct1 AS RoyaltyPct1,
        FISCAL_MO AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_EXT_DAYS_14 
    WHERE
        NOT ISNULL(DAY_DT)""")

df_15.createOrReplaceTempView("FIL_Having_15")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_16


df_16=spark.sql("""
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

df_16.createOrReplaceTempView("MA_EVENT_16")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_EVENT_17


df_17=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_AMT AS MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_16""")

df_17.createOrReplaceTempView("SQ_Shortcut_to_MA_EVENT_17")

# COMMAND ----------
# DBTITLE 1, FIL_Royalty_18


df_18=spark.sql("""
    SELECT
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        FISCAL_MO AS FISCAL_MO,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_AMT AS MA_AMT,
        MA_EVENT_ID AS MA_EVENT_ID,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_EVENT_17 
    WHERE
        MA_EVENT_TYPE_ID = 10 
        AND FISCAL_MO >= 201604""")

df_18.createOrReplaceTempView("FIL_Royalty_18")

# COMMAND ----------
# DBTITLE 1, LKP_DAYS_19


df_19=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        FIL_Royalty_18.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            (WEEK_DT + 1) AS WEEK_DT 
        FROM
            DAYS 
        WHERE
            DAY_DT = CURRENT_DATE - 96) AS DAYS 
    RIGHT OUTER JOIN
        FIL_Royalty_18 
            ON WEEK_DT < FIL_Royalty_18.START_DT""")

df_19.createOrReplaceTempView("LKP_DAYS_19")

# COMMAND ----------
# DBTITLE 1, FIL_START_DT_20


df_20=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        FISCAL_MO AS FISCAL_MO,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_AMT AS MA_AMT,
        MA_EVENT_ID AS MA_EVENT_ID,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_Royalty_18 
    WHERE
        NOT ISNULL(WEEK_DT)""")

df_20.createOrReplaceTempView("FIL_START_DT_20")

df_20=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        FISCAL_MO AS FISCAL_MO,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_AMT AS MA_AMT,
        MA_EVENT_ID AS MA_EVENT_ID,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_DAYS_19 
    WHERE
        NOT ISNULL(WEEK_DT)""")

df_20.createOrReplaceTempView("FIL_START_DT_20")

# COMMAND ----------
# DBTITLE 1, EXP_ProductId_21


df_21=spark.sql("""SELECT ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
IIF(ISNULL(PRODUCT_ID), 0, PRODUCT_ID)  AS PRODUCT_ID_JOIN,
START_DT AS START_DT,
END_DT AS END_DT,
PRODUCT_ID AS PRODUCT_ID,
FISCAL_MO AS FISCAL_MO,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_AMT AS MA_AMT,
MA_EVENT_ID AS MA_EVENT_ID,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM FIL_START_DT_20""")

df_21.createOrReplaceTempView("EXP_ProductId_21")

# COMMAND ----------
# DBTITLE 1, JNR_All_22


df_22=spark.sql("""
    SELECT
        DETAIL.ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        DETAIL.PRODUCT_ID_JOIN AS PRODUCT_ID_JOIN,
        DETAIL.START_DT AS START_DT,
        DETAIL.END_DT AS END_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.BRAND_CD AS BRAND_CD,
        DETAIL.MA_FORMULA_CD AS MA_FORMULA_CD,
        DETAIL.MA_EVENT_DESC AS MA_EVENT_DESC,
        DETAIL.MA_AMT AS MA_AMT,
        DETAIL.MA_EVENT_ID AS MA_EVENT_ID,
        DETAIL.UPDATE_DT AS UPDATE_DT,
        DETAIL.LOAD_DT AS LOAD_DT,
        MASTER.RoyaltyBrandId AS ROYALTY_BRAND_ID_MTX,
        MASTER.PRODUCT_ID_JOIN AS PRODUCT_ID_JOIN_MTX,
        MASTER.START_DT AS START_DT_MTX,
        MASTER.END_DT AS END_DT_MTX,
        MASTER.PRODUCT_ID AS PRODUCT_ID_MTX,
        MASTER.FISCAL_MO AS FISCAL_MO_MTX,
        MASTER.BrandCd AS BRAND_CD_MTX,
        MASTER.RoyaltyFormulaCd AS MA_FORMULA_CD_MTX,
        MASTER.RoyaltyBrandDesc AS MA_EVENT_DESC_MTX,
        MASTER.RoyaltyPct1 AS MA_AMT_MTX,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_Having_15 MASTER 
    LEFT JOIN
        EXP_ProductId_21 DETAIL 
            ON MASTER.RoyaltyBrandId = ROYALTY_BRAND_ID 
            AND PRODUCT_ID_JOIN_MTX = PRODUCT_ID_JOIN 
            AND START_DT_MTX = START_DT 
            AND END_DT_MTX = DETAIL.END_DT""")

df_22.createOrReplaceTempView("JNR_All_22")

# COMMAND ----------
# DBTITLE 1, EXP_Strategy_23


df_23=spark.sql("""SELECT (ROW_NUMBER() OVER(ORDER BY(SELECT NULL))-1) * (SELECT Increment_By FROM SEQ_MA_EVENT_ID) + (SELECT NEXTVAL FROM SEQ_MA_EVENT_ID) AS in_MA_EVENT_ID,
IIF (ISNULL(MA_EVENT_ID),NEXTVAL,MA_EVENT_ID) AS MA_EVENT_ID,
JNR_All_22.PRODUCT_ID_MTX AS PRODUCT_ID,
IIF (ISNULL(START_DT),JNR_All_22.START_DT_MTX,START_DT) AS START_DT,
IIF (ISNULL(END_DT),JNR_All_22.END_DT_MTX,END_DT) AS END_DT,
10 AS MA_EVENT_TYPE_ID,
9 AS MA_EVENT_SOURCE_ID,
IIF (ISNULL(ROYALTY_BRAND_ID),JNR_All_22.ROYALTY_BRAND_ID_MTX,ROYALTY_BRAND_ID) AS ROYALTY_BRAND_ID,
JNR_All_22.BRAND_CD_MTX AS BRAND_CD,
JNR_All_22.MA_FORMULA_CD_MTX AS MA_FORMULA_CD,
IIF (ISNULL(FISCAL_MO),JNR_All_22.FISCAL_MO_MTX,FISCAL_MO) AS FISCAL_MO,
JNR_All_22.MA_EVENT_DESC_MTX AS MA_EVENT_DESC,
1 AS MA_PCT_IND,
JNR_All_22.MA_AMT_MTX AS MA_AMT,
IIF (ISNULL(UPDATE_DT),SYSDATE,UPDATE_DT) AS UPDATE_DT,
TRUNC(SYSDATE) AS LOAD_DT,
DECODE(TRUE, 
ISNULL(MA_EVENT_ID),'I',

ISNULL(JNR_All_22.ROYALTY_BRAND_ID_MTX),'D',

IIF(ISNULL(BRAND_CD),'zzz ',BRAND_CD) <> IIF(ISNULL(JNR_All_22.BRAND_CD_MTX),'zzz ',JNR_All_22.BRAND_CD_MTX)
OR
IIF(ISNULL(MA_FORMULA_CD),'zzz ',MA_FORMULA_CD) <> IIF(ISNULL(JNR_All_22.MA_FORMULA_CD_MTX),'zzz ',JNR_All_22.MA_FORMULA_CD_MTX)
OR
IIF(ISNULL(MA_AMT),-1,MA_AMT) <> IIF(ISNULL(JNR_All_22.MA_AMT_MTX),-1,JNR_All_22.MA_AMT_MTX),'U',

IIF(ISNULL(MA_EVENT_DESC),'zzz ',MA_EVENT_DESC) <> IIF(ISNULL(JNR_All_22.MA_EVENT_DESC_MTX),'zzz ',JNR_All_22.MA_EVENT_DESC_MTX),'C','N') AS LOAD_FLAG,
SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM SEQ_MA_EVENT_ID INNER JOIN JNR_All_22 ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = JNR_All_22.Monotonically_Increasing_Id
""")

df_23.createOrReplaceTempView("EXP_Strategy_23")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(in_MA_EVENT_ID) FROM EXP_Strategy_23) , NEXTVAL = (SELECT MAX(in_MA_EVENT_ID) FROM EXP_Strategy_23) + (SELECT Increment_By FROM EXP_Strategy_23)""")

# COMMAND ----------
# DBTITLE 1, FIL_LoadFlag_24


df_24=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_FLAG AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Strategy_23 
    WHERE
        LOAD_FLAG <> 'N'""")

df_24.createOrReplaceTempView("FIL_LoadFlag_24")

# COMMAND ----------
# DBTITLE 1, UPD_InsertDeleteUpdate_25


df_25=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_FLAG AS Load_Flag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_LoadFlag_24""")

df_25.createOrReplaceTempView("UPD_InsertDeleteUpdate_25")

# COMMAND ----------
# DBTITLE 1, FIL_Restate_Hist_26


df_26=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        Load_Flag AS Load_Flag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Strategy_23 
    WHERE
        Load_Flag <> 'N'""")

df_26.createOrReplaceTempView("FIL_Restate_Hist_26")

# COMMAND ----------
# DBTITLE 1, EXP_Load_Flag_27


df_27=spark.sql("""SELECT TRUNC(SYSDATE) AS LOAD_DT,
MA_EVENT_ID AS MA_EVENT_ID,
PRODUCT_ID AS PRODUCT_ID,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_PCT_IND AS MA_PCT_IND,
MA_AMT AS MA_AMT,
Load_Flag AS INS_UPD_DEL_FLAG,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM FIL_Restate_Hist_26""")

df_27.createOrReplaceTempView("EXP_Load_Flag_27")

# COMMAND ----------
# DBTITLE 1, UPD_InsertOnly_28


df_28=spark.sql("""
    SELECT
        LOAD_DT AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Load_Flag_27""")

df_28.createOrReplaceTempView("UPD_InsertOnly_28")

# COMMAND ----------
# DBTITLE 1, MA_EVENT


spark.sql("""INSERT INTO MA_EVENT SELECT MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
null AS OFFER_ID,
null AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
null AS COUNTRY_CD,
START_DT AS START_DT,
START_DT AS START_DT,
END_DT AS END_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
null AS LOCATION_ID,
null AS MOVEMENT_ID,
null AS VALUATION_CLASS_CD,
null AS GL_ACCT_NBR,
null AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
null AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
null AS SOURCE_VENDOR_ID,
null AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
null AS EM_VENDOR_FUNDING_ID,
null AS EM_COMMENT,
null AS EM_BILL_ALT_VENDOR_FLAG,
null AS EM_ALT_VENDOR_ID,
null AS EM_ALT_VENDOR_NAME,
null AS EM_ALT_VENDOR_COUNTRY_CD,
null AS EM_VENDOR_ID,
null AS EM_VENDOR_NAME,
null AS EM_VENDOR_COUNTRY_CD,
null AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
null AS MA_MAX_AMT,
UPDATE_DT AS UPDATE_DT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT FROM UPD_InsertDeleteUpdate_25""")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_RESTATE_HIST


spark.sql("""INSERT INTO MA_EVENT_RESTATE_HIST SELECT LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
null AS OFFER_ID,
null AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
null AS COUNTRY_CD,
START_DT AS START_DT,
START_DT AS START_DT,
END_DT AS END_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
null AS LOCATION_ID,
null AS MOVEMENT_ID,
null AS VALUATION_CLASS_CD,
null AS GL_ACCT_NBR,
null AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
null AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
null AS SOURCE_VENDOR_ID,
null AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
null AS EM_VENDOR_FUNDING_ID,
null AS EM_COMMENT,
null AS EM_BILL_ALT_VENDOR_FLAG,
null AS EM_ALT_VENDOR_ID,
null AS EM_ALT_VENDOR_NAME,
null AS EM_ALT_VENDOR_COUNTRY_CD,
null AS EM_VENDOR_ID,
null AS EM_VENDOR_NAME,
null AS EM_VENDOR_COUNTRY_CD,
null AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
null AS MA_MAX_AMT,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG FROM UPD_InsertOnly_28""")