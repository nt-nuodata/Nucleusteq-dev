# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_EVENT_0


df_0=spark.sql("""
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

df_0.createOrReplaceTempView("MA_EVENT_0")

# COMMAND ----------
# DBTITLE 1, EM_EVENT_1


df_1=spark.sql("""
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

df_1.createOrReplaceTempView("EM_EVENT_1")

# COMMAND ----------
# DBTITLE 1, MA_MOVEMENT_DAY_2


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
        SALES_ADJ_AMT AS SALES_ADJ_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_MOVEMENT_DAY""")

df_2.createOrReplaceTempView("MA_MOVEMENT_DAY_2")

# COMMAND ----------
# DBTITLE 1, MOVEMENT_INFO_3


df_3=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
        MOVE_CLASS_ID AS MOVE_CLASS_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MOVEMENT_INFO""")

df_3.createOrReplaceTempView("MOVEMENT_INFO_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_MOVEMENT_DAY_4


df_4=spark.sql("""
    SELECT
        pre.day_dt,
        pre.product_id,
        pre.location_id,
        pre.movement_id,
        pre.po_nbr,
        pre.po_line_nbr,
        pre.ma_event_id,
        pre.sto_type_id,
        pre.ma_trans_amt,
        pre.ma_trans_cost,
        pre.ma_trans_qty,
        pre.exch_rate_pct,
        CURRENT_DATE update_dt,
        NVL(mmd.load_dt,
        CURRENT_DATE) load_dt,
        CASE 
            WHEN mmd.day_dt IS NULL THEN 'I' 
            ELSE 'U' 
        END ins_upd_flag,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            md.day_dt,
            md.product_id,
            md.location_id,
            md.movement_id,
            md.po_nbr,
            md.po_line_nbr,
            e.ma_event_id,
            md.sto_type_id,
            md.trans_amt * e.ma_amt ma_trans_amt,
            md.trans_cost * e.ma_amt ma_trans_cost,
            md.trans_qty * (e.ma_amt / ABS(e.ma_amt)) ma_trans_qty,
            md.exch_rate_pct 
        FROM
            movement_day md 
        JOIN
            movement_info mi 
                ON md.movement_id = mi.movement_id 
        JOIN
            ma_event e 
                ON md.day_dt BETWEEN e.start_dt AND e.end_dt 
                AND md.product_id = e.product_id 
        JOIN
            site_profile sp 
                ON md.location_id = sp.location_id 
                AND NVL(e.country_cd,
            sp.country_cd) = sp.country_cd 
        JOIN
            sku_profile sku 
                ON md.product_id = sku.product_id 
        LEFT JOIN
            USR_STORE_ATTRIBUTES USA 
                ON md.LOCATION_ID = USA.LOCATION_ID 
        WHERE
            e.ma_event_source_id = 5 
            AND sp.location_type_id = 8 
            AND USA.VET_TYPE_ID IN (
                1, 2
            ) 
            AND sku.valuation_class_cd = '3100' 
            AND mi.move_type_id IN (
                701, 702, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 917, 918, 921, 922, 935, 936, 937, 938
            ) 
            AND (
                (
                    md.update_dt = CURRENT_DATE 
                    AND md.day_dt < CURRENT_DATE
                ) 
                OR (
                    md.day_dt = CURRENT_DATE - 1 
                    AND md.update_dt <> CURRENT_DATE
                )
            ) 
        UNION
        SELECT
            md.day_dt,
            md.product_id,
            md.location_id,
            md.movement_id,
            md.po_nbr,
            md.po_line_nbr,
            e.ma_event_id,
            md.sto_type_id,
            md.trans_amt * (e.ma_amt / 100.0) ma_trans_amt,
            md.trans_cost * (e.ma_amt / 100.0) ma_trans_cost,
            md.trans_qty ma_trans_qty,
            md.exch_rate_pct 
        FROM
            movement_day md 
        JOIN
            sku_profile sku 
                ON md.product_id = sku.product_id 
        JOIN
            site_profile site 
                ON md.location_id = site.location_id 
        JOIN
            ma_event e 
                ON md.day_dt BETWEEN e.start_dt AND e.end_dt 
                AND md.movement_id = e.movement_id 
                AND sku.valuation_class_cd = e.valuation_class_cd 
                AND site.location_type_id = e.location_type_id 
        WHERE
            e.ma_event_source_id = 8 
            AND (
                (
                    md.update_dt = CURRENT_DATE 
                    AND md.day_dt < CURRENT_DATE
                ) 
                OR (
                    md.day_dt = CURRENT_DATE - 1 
                    AND md.update_dt <> CURRENT_DATE
                )
            )
    ) pre 
LEFT OUTER JOIN
    ma_movement_day mmd 
        ON pre.day_dt = mmd.day_dt 
        AND pre.product_id = mmd.product_id 
        AND pre.location_id = mmd.location_id 
        AND pre.movement_id = mmd.movement_id 
        AND pre.po_nbr = mmd.po_nbr 
        AND pre.po_line_nbr = mmd.po_line_nbr 
        AND pre.ma_event_id = mmd.ma_event_id""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_MA_MOVEMENT_DAY_4")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_5


df_5=spark.sql("""
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

df_5.createOrReplaceTempView("SKU_PROFILE_5")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_6


df_6=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        COMPANY_ID AS COMPANY_ID,
        REGION_ID AS REGION_ID,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        STATE_CD AS STATE_CD,
        COUNTRY_CD AS COUNTRY_CD,
        POSTAL_CD AS POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
        SITE_SALES_FLAG AS SITE_SALES_FLAG,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        PROMO_LABEL_CD AS PROMO_LABEL_CD,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
        PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
        HOTEL_FLAG AS HOTEL_FLAG,
        DAYCAMP_FLAG AS DAYCAMP_FLAG,
        VET_FLAG AS VET_FLAG,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        REGION_VP_NAME AS REGION_VP_NAME,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        SITE_COUNTY AS SITE_COUNTY,
        SITE_FAX_NO AS SITE_FAX_NO,
        SFT_OPEN_DT AS SFT_OPEN_DT,
        DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        TRADE_AREA AS TRADE_AREA,
        FDLPS_NAME AS FDLPS_NAME,
        FDLPS_EMAIL AS FDLPS_EMAIL,
        OVERSITE_MGR_NAME AS OVERSITE_MGR_NAME,
        OVERSITE_MGR_EMAIL AS OVERSITE_MGR_EMAIL,
        SAFETY_DIRECTOR_NAME AS SAFETY_DIRECTOR_NAME,
        SAFETY_DIRECTOR_EMAIL AS SAFETY_DIRECTOR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        AREA_DIRECTOR_NAME AS AREA_DIRECTOR_NAME,
        AREA_DIRECTOR_EMAIL AS AREA_DIRECTOR_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        REGIONAL_DC_SAFETY_MGR_NAME AS REGIONAL_DC_SAFETY_MGR_NAME,
        REGIONAL_DC_SAFETY_MGR_EMAIL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
        DC_PEOPLE_SUPERVISOR_NAME AS DC_PEOPLE_SUPERVISOR_NAME,
        DC_PEOPLE_SUPERVISOR_EMAIL AS DC_PEOPLE_SUPERVISOR_EMAIL,
        PEOPLE_MANAGER_NAME AS PEOPLE_MANAGER_NAME,
        PEOPLE_MANAGER_EMAIL AS PEOPLE_MANAGER_EMAIL,
        ASSET_PROT_DIR_NAME AS ASSET_PROT_DIR_NAME,
        ASSET_PROT_DIR_EMAIL AS ASSET_PROT_DIR_EMAIL,
        SR_REG_ASSET_PROT_MGR_NAME AS SR_REG_ASSET_PROT_MGR_NAME,
        SR_REG_ASSET_PROT_MGR_EMAIL AS SR_REG_ASSET_PROT_MGR_EMAIL,
        REG_ASSET_PROT_MGR_NAME AS REG_ASSET_PROT_MGR_NAME,
        REG_ASSET_PROT_MGR_EMAIL AS REG_ASSET_PROT_MGR_EMAIL,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        TP_START_DT AS TP_START_DT,
        OPEN_DT AS OPEN_DT,
        GR_OPEN_DT AS GR_OPEN_DT,
        CLOSE_DT AS CLOSE_DT,
        HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE""")

df_6.createOrReplaceTempView("SITE_PROFILE_6")

# COMMAND ----------
# DBTITLE 1, MOVEMENT_DAY_7


df_7=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        PO_NBR AS PO_NBR,
        PO_LINE_NBR AS PO_LINE_NBR,
        STO_TYPE_ID AS STO_TYPE_ID,
        SKU_STATUS_ID AS SKU_STATUS_ID,
        MOVE_CLASS_ID AS MOVE_CLASS_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        TRANS_AMT AS TRANS_AMT,
        TRANS_COST AS TRANS_COST,
        TRANS_QTY AS TRANS_QTY,
        FREIGHT_COST AS FREIGHT_COST,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        ROUND_VALUE_QTY AS ROUND_VALUE_QTY,
        ROUND_PROFILE_CD AS ROUND_PROFILE_CD,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MOVEMENT_DAY""")

df_7.createOrReplaceTempView("MOVEMENT_DAY_7")

# COMMAND ----------
# DBTITLE 1, MA_MOVEMENT_PRE


spark.sql("""INSERT INTO MA_MOVEMENT_PRE SELECT DAY_DT AS DAY_DT,
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
INS_UPD_FLAG AS INS_UPD_FLAG FROM SQ_Shortcut_to_MA_MOVEMENT_DAY_4""")