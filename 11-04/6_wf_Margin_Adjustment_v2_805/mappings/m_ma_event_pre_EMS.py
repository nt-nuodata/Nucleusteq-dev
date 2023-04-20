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
# DBTITLE 1, MA_EVENT_TYPE_1


df_1=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_TYPE_DESC AS MA_EVENT_TYPE_DESC,
        REF_MA_EVENT_TYPE_ID AS REF_MA_EVENT_TYPE_ID,
        EM_TPR_TYPE AS EM_TPR_TYPE,
        PETPERK_ONLY_FLAG AS PETPERK_ONLY_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_TYPE""")

df_1.createOrReplaceTempView("MA_EVENT_TYPE_1")

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
# DBTITLE 1, EM_EVENT_3


df_3=spark.sql("""
    SELECT
        EM_EVENT_ID AS EM_EVENT_ID,
        EVENT_NAME AS EVENT_NAME,
        EVENT_DESC AS EVENT_DESC,
        EVENT_START_DT AS EVENT_START_DT,
        EVENT_END_DT AS EVENT_END_DT,
        EVENT_LOCK_DT AS EVENT_LOCK_DT,
        EVENT_US_LOCK_DT AS EVENT_US_LOCK_DT,
        EVENT_TURN_IN_DT AS EVENT_TURN_IN_DT,
        EVENT_DEFAULT_START_DT AS EVENT_DEFAULT_START_DT,
        EVENT_DEFAULT_END_DT AS EVENT_DEFAULT_END_DT,
        POG_ID_SUFFIX AS POG_ID_SUFFIX,
        EM_MODIFIED_BY AS EM_MODIFIED_BY,
        EM_MODIFIED_DT AS EM_MODIFIED_DT,
        STANDARD_FLAG AS STANDARD_FLAG,
        GENERATE_PRICING_FILES_FLAG AS GENERATE_PRICING_FILES_FLAG,
        EM_PURGED_FLAG AS EM_PURGED_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EM_EVENT""")

df_3.createOrReplaceTempView("EM_EVENT_3")

# COMMAND ----------
# DBTITLE 1, ASQ_MA_EVENT_PRE_NEW_4


df_4=spark.sql("""
    SELECT
        vf.product_id,
        vf.country_cd,
        vf.start_dt,
        vf.end_dt,
        met.ma_event_type_id,
        1 ma_event_source_id,
        d.ma_event_desc,
        vf.em_vendor_funding_id,
        vf.em_comment,
        vf.bill_alternate_vendor_flag em_bill_alt_vendor_flag,
        vf.alternate_vendor_id em_alt_vendor_id,
        CASE 
            WHEN vf.alternate_vendor_id IS NOT NULL THEN NVL(av.vendor_name,
            'UNKNOWN') 
        END em_alt_vendor_name,
        av.country_cd em_alt_vendor_country_cd,
        es.vendor_id em_vendor_id,
        es.vendor_name em_vendor_name,
        v.country_cd em_vendor_country_cd,
        vf.vf_amt,
        CASE 
            WHEN vf.max_amt = 0 THEN NULL 
            ELSE vf.max_amt 
        END ma_max_amt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        em_vendor_funding vf 
    JOIN
        em_event e 
            ON vf.em_event_id = e.em_event_id 
    JOIN
        em_event_sku es 
            ON vf.em_event_id = es.em_event_id 
            AND vf.product_id = es.product_id 
            AND vf.country_cd = es.country_cd 
    JOIN
        ma_event_type met 
            ON met.em_tpr_type = vf.vf_type 
    JOIN
        (
            SELECT
                DISTINCT vf.em_vendor_funding_id,
                e.event_name || ' - SKU=' || CAST(sp.sku_nbr AS CHAR (7)) ma_event_desc 
            FROM
                em_vendor_funding vf,
                em_event e,
                sku_profile sp 
            WHERE
                vf.em_event_id = e.em_event_id 
                AND vf.product_id = sp.product_id
        ) d 
            ON vf.em_vendor_funding_id = d.em_vendor_funding_id 
    LEFT JOIN
        (
            SELECT
                CAST(vendor_id AS CHARACTER VARYING (25)) vendor_id,
                vendor_name,
                country_cd 
            FROM
                vendor_profile 
            WHERE
                vendor_type_id = 2
        ) av 
            ON RTRIM(LTRIM(vf.alternate_vendor_id)) = av.vendor_id 
    LEFT JOIN
        (
            SELECT
                vendor_id,
                vendor_name,
                country_cd 
            FROM
                vendor_profile 
            WHERE
                vendor_type_id = 2
        ) v 
            ON es.vendor_id = v.vendor_id 
    WHERE
        es.promo_flag = 1 
        AND vf.start_dt >= (
            SELECT
                week_dt + 1 
            FROM
                days 
            WHERE
                day_dt = CURRENT_DATE - 96
        ) 
        AND vf.vf_amt > 0 
        AND (
            vf.em_vendor_funding_id, vf.country_cd
        ) NOT IN (
            SELECT
                DISTINCT em_vendor_funding_id,
                country_cd 
            FROM
                ma_event 
            WHERE
                ma_event_source_id = 1
        )""")

df_4.createOrReplaceTempView("ASQ_MA_EVENT_PRE_NEW_4")

# COMMAND ----------
# DBTITLE 1, EXP_NEW_5


df_5=spark.sql("""
    SELECT
        (ROW_NUMBER() OVER (
    ORDER BY
        (SELECT
            NULL)) - 1) * (SELECT
            Increment_By 
        FROM
            SEQ_MA_EVENT_ID) + (SELECT
            NEXTVAL 
        FROM
            SEQ_MA_EVENT_ID) AS MA_EVENT_ID,
        NEXTVAL AS MA_EVENT_ID,
        ASQ_MA_EVENT_PRE_NEW_4.PRODUCT_ID AS PRODUCT_ID,
        ASQ_MA_EVENT_PRE_NEW_4.COUNTRY_CD AS COUNTRY_CD,
        ASQ_MA_EVENT_PRE_NEW_4.START_DT AS START_DT,
        ASQ_MA_EVENT_PRE_NEW_4.END_DT AS END_DT,
        ASQ_MA_EVENT_PRE_NEW_4.MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        ASQ_MA_EVENT_PRE_NEW_4.MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ASQ_MA_EVENT_PRE_NEW_4.MA_EVENT_DESC AS MA_EVENT_DESC,
        ASQ_MA_EVENT_PRE_NEW_4.EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        ASQ_MA_EVENT_PRE_NEW_4.EM_COMMENT AS EM_COMMENT,
        ASQ_MA_EVENT_PRE_NEW_4.EM_BILL_ALT_VENDOR AS EM_BILL_ALT_VENDOR,
        ASQ_MA_EVENT_PRE_NEW_4.EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        ASQ_MA_EVENT_PRE_NEW_4.EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        ASQ_MA_EVENT_PRE_NEW_4.EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        ASQ_MA_EVENT_PRE_NEW_4.EM_VENDOR_ID AS EM_VENDOR_ID,
        ASQ_MA_EVENT_PRE_NEW_4.EM_VENDOR_NAME AS EM_VENDOR_NAME,
        ASQ_MA_EVENT_PRE_NEW_4.EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        ASQ_MA_EVENT_PRE_NEW_4.MA_AMT AS MA_AMT,
        ASQ_MA_EVENT_PRE_NEW_4.MA_MAX_AMT AS MA_MAX_AMT,
        SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_MA_EVENT_ID 
    INNER JOIN
        ASQ_MA_EVENT_PRE_NEW_4 
            ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = ASQ_MA_EVENT_PRE_NEW_4.Monotonically_Increasing_Id""")

df_5.createOrReplaceTempView("EXP_NEW_5")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_5) , NEXTVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_5) + (SELECT Increment_By FROM EXP_NEW_5)""")

# COMMAND ----------
# DBTITLE 1, ASQ_MA_EVENT_PRE_EXISTING_6


df_6=spark.sql("""
    SELECT
        me.ma_event_id,
        vf.product_id,
        vf.country_cd,
        vf.start_dt,
        vf.end_dt,
        met.ma_event_type_id,
        1 ma_event_source_id,
        d.ma_event_desc,
        vf.em_vendor_funding_id,
        vf.em_comment,
        vf.bill_alternate_vendor_flag em_bill_alt_vendor_flag,
        vf.alternate_vendor_id em_alt_vendor_id,
        CASE 
            WHEN vf.alternate_vendor_id IS NOT NULL THEN NVL(av.vendor_name,
            'UNKNOWN') 
        END em_alt_vendor_name,
        av.country_cd em_alt_vendor_country_cd,
        es.vendor_id em_vendor_id,
        es.vendor_name em_vendor_name,
        v.country_cd em_vendor_country_cd,
        vf.vf_amt,
        CASE 
            WHEN vf.max_amt = 0 THEN NULL 
            ELSE vf.max_amt 
        END ma_max_amt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        em_vendor_funding vf 
    JOIN
        ma_event me 
            ON vf.em_vendor_funding_id = me.em_vendor_funding_id 
            AND vf.country_cd = me.country_cd 
    JOIN
        em_event e 
            ON vf.em_event_id = e.em_event_id 
    JOIN
        em_event_sku es 
            ON vf.em_event_id = es.em_event_id 
            AND vf.product_id = es.product_id 
            AND vf.country_cd = es.country_cd 
    JOIN
        ma_event_type met 
            ON met.em_tpr_type = vf.vf_type 
    JOIN
        (
            SELECT
                DISTINCT vf.em_vendor_funding_id,
                e.event_name || ' - SKU=' || CAST(sp.sku_nbr AS CHAR (7)) ma_event_desc 
            FROM
                em_vendor_funding vf,
                em_event e,
                sku_profile sp 
            WHERE
                vf.em_event_id = e.em_event_id 
                AND vf.product_id = sp.product_id
        ) d 
            ON vf.em_vendor_funding_id = d.em_vendor_funding_id 
    LEFT JOIN
        (
            SELECT
                CAST(vendor_id AS CHARACTER VARYING (25)) vendor_id,
                vendor_name,
                country_cd 
            FROM
                vendor_profile 
            WHERE
                vendor_type_id = 2
        ) av 
            ON RTRIM(LTRIM(vf.alternate_vendor_id)) = av.vendor_id 
    LEFT JOIN
        (
            SELECT
                vendor_id,
                vendor_name,
                country_cd 
            FROM
                vendor_profile 
            WHERE
                vendor_type_id = 2
        ) v 
            ON es.vendor_id = v.vendor_id 
    WHERE
        es.promo_flag = 1 
        AND vf.start_dt >= (
            SELECT
                week_dt + 1 
            FROM
                days 
            WHERE
                day_dt = CURRENT_DATE - 96
        ) 
        AND vf.vf_amt > 0""")

df_6.createOrReplaceTempView("ASQ_MA_EVENT_PRE_EXISTING_6")

# COMMAND ----------
# DBTITLE 1, UNI_EMS_7


df_7=spark.sql("""SELECT COUNTRY_CD AS COUNTRY_CD,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_BILL_ALT_VENDOR AS EM_BILL_ALT_VENDOR,
EM_COMMENT AS EM_COMMENT,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
END_DT AS END_DT,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_MAX_AMT AS MA_MAX_AMT,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PRODUCT_ID AS PRODUCT_ID,
START_DT AS START_DT FROM ASQ_MA_EVENT_PRE_EXISTING_6 UNION ALL SELECT COUNTRY_CD AS COUNTRY_CD,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_BILL_ALT_VENDOR AS EM_BILL_ALT_VENDOR,
EM_COMMENT AS EM_COMMENT,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
END_DT AS END_DT,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_MAX_AMT AS MA_MAX_AMT,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PRODUCT_ID AS PRODUCT_ID,
START_DT AS START_DT FROM EXP_NEW_5""")

df_7.createOrReplaceTempView("UNI_EMS_7")

# COMMAND ----------
# DBTITLE 1, EM_VENDOR_FUNDING_8


df_8=spark.sql("""
    SELECT
        EM_EVENT_ID AS EM_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        EM_ARTICLE_ID AS EM_ARTICLE_ID,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        VENDOR_NAME AS VENDOR_NAME,
        VF_TYPE AS VF_TYPE,
        VF_AMT AS VF_AMT,
        MAX_AMT AS MAX_AMT,
        BILL_ALTERNATE_VENDOR_FLAG AS BILL_ALTERNATE_VENDOR_FLAG,
        ALTERNATE_VENDOR_ID AS ALTERNATE_VENDOR_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        EM_COMMENT AS EM_COMMENT,
        EM_MODIFIED_BY AS EM_MODIFIED_BY,
        EM_MODIFIED_DT AS EM_MODIFIED_DT,
        EM_VF_MODIFIED_BY AS EM_VF_MODIFIED_BY,
        EM_VF_MODIFIED_DT AS EM_VF_MODIFIED_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EM_VENDOR_FUNDING""")

df_8.createOrReplaceTempView("EM_VENDOR_FUNDING_8")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_9


df_9=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_TYPE AS SKU_TYPE,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        STATUS_ID AS STATUS_ID,
        SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
        SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
        SKU_DESC AS SKU_DESC,
        ALT_DESC AS ALT_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        COUNTRY_CD AS COUNTRY_CD,
        IMPORT_FLAG AS IMPORT_FLAG,
        HTS_CODE_ID AS HTS_CODE_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        SIZE_DESC AS SIZE_DESC,
        BUM_QTY AS BUM_QTY,
        UOM_CD AS UOM_CD,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        BUYER_ID AS BUYER_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_COST_AMT AS PURCH_COST_AMT,
        NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BRAND_CD AS BRAND_CD,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        INIT_MKDN_DT AS INIT_MKDN_DT,
        DISC_START_DT AS DISC_START_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        FIRST_INV_DT AS FIRST_INV_DT,
        LAST_INV_DT AS LAST_INV_DT,
        LOAD_DT AS LOAD_DT,
        BASE_NBR AS BASE_NBR,
        BP_COLOR_ID AS BP_COLOR_ID,
        BP_SIZE_ID AS BP_SIZE_ID,
        BP_BREED_ID AS BP_BREED_ID,
        BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
        BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
        BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
        NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        COMPONENT_FLAG AS COMPONENT_FLAG,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_PID_DT AS ZDISCO_PID_DT,
        ZDISCO_START_DT AS ZDISCO_START_DT,
        ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
        ZDISCO_DC_DT AS ZDISCO_DC_DT,
        ZDISCO_STR_DT AS ZDISCO_STR_DT,
        ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
        ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE""")

df_9.createOrReplaceTempView("SKU_PROFILE_9")

# COMMAND ----------
# DBTITLE 1, EM_EVENT_SKU_10


df_10=spark.sql("""
    SELECT
        EM_EVENT_ID AS EM_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        EM_ARTICLE_ID AS EM_ARTICLE_ID,
        SKU_NBR AS SKU_NBR,
        BUYER_NAME AS BUYER_NAME,
        NATL_PRICE AS NATL_PRICE,
        MAP_AMT AS MAP_AMT,
        DISP_RANK_A_QTY AS DISP_RANK_A_QTY,
        DISP_RANK_B_QTY AS DISP_RANK_B_QTY,
        DISP_RANK_C_QTY AS DISP_RANK_C_QTY,
        DISP_RANK_D_QTY AS DISP_RANK_D_QTY,
        DISP_RANK_E_QTY AS DISP_RANK_E_QTY,
        DISP_RANK_F_QTY AS DISP_RANK_F_QTY,
        NON_DISP_RANK_A_QTY AS NON_DISP_RANK_A_QTY,
        NON_DISP_RANK_B_QTY AS NON_DISP_RANK_B_QTY,
        NON_DISP_RANK_C_QTY AS NON_DISP_RANK_C_QTY,
        NON_DISP_RANK_D_QTY AS NON_DISP_RANK_D_QTY,
        NON_DISP_RANK_E_QTY AS NON_DISP_RANK_E_QTY,
        NON_DISP_RANK_F_QTY AS NON_DISP_RANK_F_QTY,
        DISCOUNT_TYPE AS DISCOUNT_TYPE,
        ACTUAL_AMT AS ACTUAL_AMT,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        VENDOR_NAME AS VENDOR_NAME,
        CREATED_DT AS CREATED_DT,
        ALLOCATION_TYPE AS ALLOCATION_TYPE,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SAP_DEPARTMENT_DESC AS SAP_DEPARTMENT_DESC,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SKU_DESC AS SKU_DESC,
        CONTENTS AS CONTENTS,
        UOM_DESC AS UOM_DESC,
        STATUS_ID AS STATUS_ID,
        VENDOR_ID AS VENDOR_ID,
        BRAND_NAME AS BRAND_NAME,
        CREATED_BY AS CREATED_BY,
        PROMO_FLAG AS PROMO_FLAG,
        PROMO_START_DT AS PROMO_START_DT,
        PROMO_END_DT AS PROMO_END_DT,
        DISP_UPLIFT_PCT AS DISP_UPLIFT_PCT,
        NONDISP_UPLIFT_PCT AS NONDISP_UPLIFT_PCT,
        MIN_PRES_QTY AS MIN_PRES_QTY,
        POS_START_DT AS POS_START_DT,
        POS_END_DT AS POS_END_DT,
        VENDOR_PURCHGRP_NAME AS VENDOR_PURCHGRP_NAME,
        MODE_PRICE AS MODE_PRICE,
        MODE_COST AS MODE_COST,
        LIKE_ARTICLE_SKU AS LIKE_ARTICLE_SKU,
        PAST_PROMO_NAME AS PAST_PROMO_NAME,
        DISPLAY_INITIAL_PLANNER AS DISPLAY_INITIAL_PLANNER,
        DISPLAY_FINAL_PLANNER AS DISPLAY_FINAL_PLANNER,
        DISPLAY_PLANNER_QTY AS DISPLAY_PLANNER_QTY,
        EXIT_STRGY_FROM_STATUS_CD AS EXIT_STRGY_FROM_STATUS_CD,
        EXIT_STRGY_TO_STATUS_CD AS EXIT_STRGY_TO_STATUS_CD,
        EXIT_STRGY_MSG AS EXIT_STRGY_MSG,
        MKTG_VARIETY_STYLE_TYPE AS MKTG_VARIETY_STYLE_TYPE,
        MKTG_PRIMARY_STORY_TYPE AS MKTG_PRIMARY_STORY_TYPE,
        MKTG_SECONDARY_STORY_TYPE AS MKTG_SECONDARY_STORY_TYPE,
        MKTG_EXCLUSIVE_TO_PETM_FLAG AS MKTG_EXCLUSIVE_TO_PETM_FLAG,
        MKTG_NEW_PHOTO_REQUIRED_FLAG AS MKTG_NEW_PHOTO_REQUIRED_FLAG,
        MKTG_PRIMARY_SIGN_TYPE AS MKTG_PRIMARY_SIGN_TYPE,
        MKTG_SECONDARY_SIGN_TYPE AS MKTG_SECONDARY_SIGN_TYPE,
        MKTG_DISPLAY_LOCATION_MSG AS MKTG_DISPLAY_LOCATION_MSG,
        MKTG_HAS_COUPON_FLAG AS MKTG_HAS_COUPON_FLAG,
        MKTG_COUPON_LOCATION AS MKTG_COUPON_LOCATION,
        MKTG_COUPON_DETAILS AS MKTG_COUPON_DETAILS,
        MKTG_HAS_BONUS_FLAG AS MKTG_HAS_BONUS_FLAG,
        MKTG_BONUS_LOCATION AS MKTG_BONUS_LOCATION,
        MKTG_BONUS_DETAILS AS MKTG_BONUS_DETAILS,
        MKTG_ON_RADIO_FLAG AS MKTG_ON_RADIO_FLAG,
        MKTG_ON_TV_FLAG AS MKTG_ON_TV_FLAG,
        ALLOC_EXP_ALLOC_REQUIRED_FLAG AS ALLOC_EXP_ALLOC_REQUIRED_FLAG,
        ALLOC_EXP_MIN_QTY AS ALLOC_EXP_MIN_QTY,
        ALLOC_EXP_MAX_QTY AS ALLOC_EXP_MAX_QTY,
        ALLOC_EXP_ANTCPTD_LIFT_PCT AS ALLOC_EXP_ANTCPTD_LIFT_PCT,
        ALLOC_EXP_VENDOR_CMTD_QTY AS ALLOC_EXP_VENDOR_CMTD_QTY,
        ALLOC_EXP_LIKE_ARTICLE_SKU AS ALLOC_EXP_LIKE_ARTICLE_SKU,
        ALLOC_EXP_PAST_PROMO_NAME AS ALLOC_EXP_PAST_PROMO_NAME,
        STORE_LISTING_COUNT AS STORE_LISTING_COUNT,
        COPYEVENTID AS COPYEVENTID,
        COPYHISTORYWITHCOPYEVENT AS COPYHISTORYWITHCOPYEVENT,
        COPYHISTORYEVENTWEIGHTING AS COPYHISTORYEVENTWEIGHTING,
        COPYHISTORYSALESWEIGHTING AS COPYHISTORYSALESWEIGHTING,
        COPYHISTORYPERCENTLIFT AS COPYHISTORYPERCENTLIFT,
        COPYHISTORYBEGINDATE AS COPYHISTORYBEGINDATE,
        COPYHISTORYENDDATE AS COPYHISTORYENDDATE,
        COPYHISTORYWEEKS AS COPYHISTORYWEEKS,
        COPYHISTORYHISTORICEVENTSMONTHS AS COPYHISTORYHISTORICEVENTSMONTHS,
        SHIPPERCOMPONENTNUMBER AS SHIPPERCOMPONENTNUMBER,
        SHIPPERSELLTHRUPERCENT AS SHIPPERSELLTHRUPERCENT,
        SHIPPERUSEALLOCATIONQUANTITY AS SHIPPERUSEALLOCATIONQUANTITY,
        SHIPPERMAXSHIPPERS AS SHIPPERMAXSHIPPERS,
        TOPDOWNMINQUANTITY AS TOPDOWNMINQUANTITY,
        TOPDOWNRANKTYPE AS TOPDOWNRANKTYPE,
        TOPDOWNROUNDINGPROFILE AS TOPDOWNROUNDINGPROFILE,
        TOPDOWNVENDORQTY AS TOPDOWNVENDORQTY,
        LIKESKU AS LIKESKU,
        NOSAPEXPORT AS NOSAPEXPORT,
        SUBTRACTDPR AS SUBTRACTDPR,
        PROMOTRACKINGENABLED AS PROMOTRACKINGENABLED,
        EM_MODIFIED_BY AS EM_MODIFIED_BY,
        EM_MODIFIED_DT AS EM_MODIFIED_DT,
        EM_ALLOC_MODIFIED_BY AS EM_ALLOC_MODIFIED_BY,
        EM_ALLOC_MODIFIED_DT AS EM_ALLOC_MODIFIED_DT,
        EM_ALLOC_EXP_MODIFIED_BY AS EM_ALLOC_EXP_MODIFIED_BY,
        EM_ALLOC_EXP_MODIFIED_DT AS EM_ALLOC_EXP_MODIFIED_DT,
        EM_DISP_MODIFIED_BY AS EM_DISP_MODIFIED_BY,
        EM_DISP_MODIFIED_DT AS EM_DISP_MODIFIED_DT,
        EM_EXIT_STRGY_MODIFIED_BY AS EM_EXIT_STRGY_MODIFIED_BY,
        EM_EXIT_STRGY_MODIFIED_DT AS EM_EXIT_STRGY_MODIFIED_DT,
        EM_GENERAL_MODIFIED_BY AS EM_GENERAL_MODIFIED_BY,
        EM_GENERAL_MODIFIED_DT AS EM_GENERAL_MODIFIED_DT,
        EM_MKTG_MODIFIED_BY AS EM_MKTG_MODIFIED_BY,
        EM_MKTG_MODIFIED_DT AS EM_MKTG_MODIFIED_DT,
        EM_PRICING_MODIFIED_BY AS EM_PRICING_MODIFIED_BY,
        EM_PRICING_MODIFIED_DT AS EM_PRICING_MODIFIED_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EM_EVENT_SKU""")

df_10.createOrReplaceTempView("EM_EVENT_SKU_10")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_PRE


spark.sql("""INSERT INTO MA_EVENT_PRE SELECT MA_EVENT_ID AS MA_EVENT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
null AS LOCATION_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_COMMENT AS EM_COMMENT,
EM_BILL_ALT_VENDOR AS EM_BILL_ALT_VENDOR_FLAG,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
null AS UPDATE_DT,
null AS LOAD_DT FROM UNI_EMS_7""")