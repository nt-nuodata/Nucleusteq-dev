# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_EVENT_TYPE_VAR_CTRL_0


df_0=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_TYPE_VAR_TYPE_CD AS MA_EVENT_TYPE_VAR_TYPE_CD,
        MA_EVENT_TYPE_VAR_VALUE AS MA_EVENT_TYPE_VAR_VALUE,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_TYPE_VAR_CTRL""")

df_0.createOrReplaceTempView("MA_EVENT_TYPE_VAR_CTRL_0")

# COMMAND ----------
# DBTITLE 1, MA_DC_COST_ADJ_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        EST_DC_COST_USD AS EST_DC_COST_USD,
        ACT_DC_COST_USD AS ACT_DC_COST_USD,
        DC_COST_ADJ_PCT AS DC_COST_ADJ_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_DC_COST_ADJ_CTRL""")

df_1.createOrReplaceTempView("MA_DC_COST_ADJ_CTRL_1")

# COMMAND ----------
# DBTITLE 1, MA_GL_ACCT_CTRL_2


df_2=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        DC_COST_OVERRIDE_IND AS DC_COST_OVERRIDE_IND,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_GL_ACCT_CTRL""")

df_2.createOrReplaceTempView("MA_GL_ACCT_CTRL_2")

# COMMAND ----------
# DBTITLE 1, MA_FISCAL_MO_CTRL_3


df_3=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        RESTATE_DT AS RESTATE_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_FISCAL_MO_CTRL""")

df_3.createOrReplaceTempView("MA_FISCAL_MO_CTRL_3")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_TYPE_4


df_4=spark.sql("""
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

df_4.createOrReplaceTempView("MA_EVENT_TYPE_4")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_5


df_5=spark.sql("""
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

df_5.createOrReplaceTempView("MA_EVENT_5")

# COMMAND ----------
# DBTITLE 1, MA_SALES_TRANS_UPC_6


df_6=spark.sql("""
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
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_SALES_TRANS_UPC""")

df_6.createOrReplaceTempView("MA_SALES_TRANS_UPC_6")

# COMMAND ----------
# DBTITLE 1, GL_ACCOUNT_PROFILE_7


df_7=spark.sql("""
    SELECT
        GL_ACCOUNT_GID AS GL_ACCOUNT_GID,
        GL_CHART_OF_ACCOUNTS_CD AS GL_CHART_OF_ACCOUNTS_CD,
        GL_ACCOUNT_NBR AS GL_ACCOUNT_NBR,
        GL_ACCOUNT_DESC AS GL_ACCOUNT_DESC,
        GL_ACCOUNT_GROUP_CD AS GL_ACCOUNT_GROUP_CD,
        GL_ACCOUNT_GROUP_DESC AS GL_ACCOUNT_GROUP_DESC,
        GL_BAL_SHEET_IND AS GL_BAL_SHEET_IND,
        GL_PL_IND AS GL_PL_IND,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_ACCOUNT_PROFILE""")

df_7.createOrReplaceTempView("GL_ACCOUNT_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, GL_PROFIT_CENTER_PROFILE_8


df_8=spark.sql("""
    SELECT
        GL_PROFIT_CENTER_GID AS GL_PROFIT_CENTER_GID,
        GL_COMPANY_CD AS GL_COMPANY_CD,
        GL_PROFIT_CENTER_CD AS GL_PROFIT_CENTER_CD,
        GL_PROFIT_CENTER_DESC AS GL_PROFIT_CENTER_DESC,
        GL_HIERARCHY_AREA AS GL_HIERARCHY_AREA,
        VALID_FROM_DT AS VALID_FROM_DT,
        VALID_TO_DT AS VALID_TO_DT,
        CURRENCY_ID AS CURRENCY_ID,
        LOCATION_ID AS LOCATION_ID,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_PROFIT_CENTER_PROFILE""")

df_8.createOrReplaceTempView("GL_PROFIT_CENTER_PROFILE_8")

# COMMAND ----------
# DBTITLE 1, DAYS_9


df_9=spark.sql("""
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

df_9.createOrReplaceTempView("DAYS_9")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_RPT_10


df_10=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        LOCATION_TYPE_DESC AS LOCATION_TYPE_DESC,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        COMPANY_ID AS COMPANY_ID,
        COMPANY_DESC AS COMPANY_DESC,
        SUPER_REGION_ID AS SUPER_REGION_ID,
        SUPER_REGION_DESC AS SUPER_REGION_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_COUNTY AS SITE_COUNTY,
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        POSTAL_CD AS POSTAL_CD,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_FAX_NO AS SITE_FAX_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        SFT_OPEN_DT AS SFT_OPEN_DT,
        OPEN_DT AS OPEN_DT,
        GR_OPEN_DT AS GR_OPEN_DT,
        CLOSE_DT AS CLOSE_DT,
        SITE_SALES_FLAG AS SITE_SALES_FLAG,
        SALES_CURR_FLAG AS SALES_CURR_FLAG,
        SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        FIRST_MEASURED_SALE_DT AS FIRST_MEASURED_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        COMP_CURR_FLAG AS COMP_CURR_FLAG,
        COMP_EFF_DT AS COMP_EFF_DT,
        COMP_END_DT AS COMP_END_DT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        HOTEL_FLAG AS HOTEL_FLAG,
        HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
        DAYCAMP_FLAG AS DAYCAMP_FLAG,
        VET_FLAG AS VET_FLAG,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        TIME_ZONE AS TIME_ZONE,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        TRADE_AREA AS TRADE_AREA,
        DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
        PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        PROMO_LABEL_CD AS PROMO_LABEL_CD,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_DESC AS EQUINE_SITE_DESC,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_MANAGER_NAME AS SITE_MANAGER_NAME,
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        TOTAL_SALES_RANKING_CD AS TOTAL_SALES_RANKING_CD,
        MERCH_SALES_RANKING_CD AS MERCH_SALES_RANKING_CD,
        SERVICES_SALES_RANKING_CD AS SERVICES_SALES_RANKING_CD,
        SALON_SALES_RANKING_CD AS SALON_SALES_RANKING_CD,
        TRAINING_SALES_RANKING_CD AS TRAINING_SALES_RANKING_CD,
        HOTEL_DDC_SALES_RANKING_CD AS HOTEL_DDC_SALES_RANKING_CD,
        CONSUMABLES_SALES_RANKING_CD AS CONSUMABLES_SALES_RANKING_CD,
        HARDGOODS_SALES_RANKING_CD AS HARDGOODS_SALES_RANKING_CD,
        SPECIALTY_SALES_RANKING_CD AS SPECIALTY_SALES_RANKING_CD,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
        DC_AREA_DIRECTOR_NAME AS DC_AREA_DIRECTOR_NAME,
        DC_AREA_DIRECTOR_EMAIL AS DC_AREA_DIRECTOR_EMAIL,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        REGION_VP_NAME AS REGION_VP_NAME,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        LP_SAFETY_DIRECTOR_NAME AS LP_SAFETY_DIRECTOR_NAME,
        LP_SAFETY_DIRECTOR_EMAIL AS LP_SAFETY_DIRECTOR_EMAIL,
        SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
        SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
        REGIONAL_LP_SAFETY_MGR_NAME AS REGIONAL_LP_SAFETY_MGR_NAME,
        REGIONAL_LP_SAFETY_MGR_EMAIL AS REGIONAL_LP_SAFETY_MGR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        HR_MANAGER_NAME AS HR_MANAGER_NAME,
        HR_MANAGER_EMAIL AS HR_MANAGER_EMAIL,
        HR_SUPERVISOR_NAME1 AS HR_SUPERVISOR_NAME1,
        HR_SUPERVISOR_EMAIL1 AS HR_SUPERVISOR_EMAIL1,
        HR_SUPERVISOR_NAME2 AS HR_SUPERVISOR_NAME2,
        HR_SUPERVISOR_EMAIL2 AS HR_SUPERVISOR_EMAIL2,
        LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
        LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_RPT""")

df_10.createOrReplaceTempView("SITE_PROFILE_RPT_10")

# COMMAND ----------
# DBTITLE 1, SQ_MA_DC_COST_ADJ_CTRL_11


df_11=spark.sql("""
    SELECT
        A.FISCAL_MO,
        A.MA_EVENT_TYPE_ID,
        A.EST_DC_COST_USD,
        A.ACT_DC_COST_USD,
        A.DC_COST_ADJ_PCT,
        CURRENT_TIMESTAMP UPDATE_TSTMP,
        NVL(A.LOAD_TSTMP,
        CURRENT_TIMESTAMP) LOAD_TSTMP,
        CASE 
            WHEN MDCAC.FISCAL_MO IS NULL THEN 'I' 
            WHEN A.DC_COST_ADJ_PCT <> MDCAC.DC_COST_ADJ_PCT THEN 'U' 
            ELSE 'X' 
        END INS_UPD_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            A.FISCAL_MO,
            B.MA_EVENT_TYPE_ID,
            A.EST_DC_COST_USD,
            B.ACT_DC_COST_USD,
            ((B.ACT_DC_COST_USD - A.EST_DC_COST_USD) / A.EST_DC_COST_USD)::NUMERIC (12,
            6) AS DC_COST_ADJ_PCT,
            CURRENT_TIMESTAMP UPDATE_TSTMP,
            CURRENT_TIMESTAMP LOAD_TSTMP 
        FROM
            (SELECT
                ME.FISCAL_MO,
                ME.MA_EVENT_TYPE_ID,
                MET.REF_MA_EVENT_TYPE_ID,
                -SUM(MSTU.MA_SALES_AMT * EXCH_RATE_PCT)::NUMERIC (12,
                2) EST_DC_COST_USD 
            FROM
                MA_SALES_TRANS_UPC MSTU 
            JOIN
                MA_EVENT ME 
                    ON MSTU.MA_EVENT_ID = ME.MA_EVENT_ID 
            JOIN
                MA_FISCAL_MO_CTRL MFMC 
                    ON ME.FISCAL_MO = MFMC.FISCAL_MO 
                    AND MFMC.RESTATE_DT = CURRENT_DATE 
            JOIN
                MA_EVENT_TYPE MET 
                    ON ME.MA_EVENT_TYPE_ID = MET.MA_EVENT_TYPE_ID 
            WHERE
                ME.MA_EVENT_SOURCE_ID = 10 
            GROUP BY
                ME.FISCAL_MO,
                ME.MA_EVENT_TYPE_ID,
                MET.REF_MA_EVENT_TYPE_ID) A 
        JOIN
            (
                SELECT
                    NVL(C.MA_EVENT_TYPE_ID,
                    D.MA_EVENT_TYPE_ID) MA_EVENT_TYPE_ID,
                    NVL(C.FISCAL_MO,
                    D.FISCAL_MO) FISCAL_MO,
                    NVL(C.ACT_DC_COST_USD,
                    0) + NVL(D.ACT_DC_COST_USD,
                    0) ACT_DC_COST_USD 
                FROM
                    (SELECT
                        MET.MA_EVENT_TYPE_ID,
                        GAD.FISCAL_MO,
                        SUM(GAD.GL_GRP_AMT)::NUMERIC (12,
                        2) ACT_DC_COST_USD 
                    FROM
                        GL_ACTUAL_DETAIL GAD 
                    JOIN
                        GL_ACCOUNT_PROFILE GAP 
                            ON GAD.GL_ACCOUNT_GID = GAP.GL_ACCOUNT_GID 
                    LEFT JOIN
                        DM_GL_COA DGC 
                            ON GAP.GL_ACCOUNT_NBR = DGC.GL_ACCT_NBR 
                    JOIN
                        SITE_PROFILE_RPT SITE 
                            ON GAD.LOCATION_ID = SITE.LOCATION_ID 
                    JOIN
                        GL_PROFIT_CENTER_PROFILE GPCP 
                            ON GAD.GL_PROFIT_CENTER_GID = GPCP.GL_PROFIT_CENTER_GID 
                    JOIN
                        MA_FISCAL_MO_CTRL MFMC 
                            ON GAD.FISCAL_MO = MFMC.FISCAL_MO 
                            AND MFMC.RESTATE_DT = CURRENT_DATE CROSS 
                    JOIN
                        (
                            SELECT
                                DISTINCT MA_EVENT_TYPE_ID 
                            FROM
                                MA_EVENT_TYPE_VAR_CTRL 
                            WHERE
                                MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                        ) MET 
                    LEFT JOIN
                        (
                            SELECT
                                MA_EVENT_TYPE_ID,
                                MA_EVENT_TYPE_VAR_VALUE,
                                START_EFF_DT,
                                END_EFF_DT 
                            FROM
                                MA_EVENT_TYPE_VAR_CTRL 
                            WHERE
                                MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_STORE_LNBR_INCL' 
                                AND MA_EVENT_TYPE_ID IN (
                                    SELECT
                                        DISTINCT MA_EVENT_TYPE_ID 
                                    FROM
                                        MA_EVENT_TYPE_VAR_CTRL 
                                    WHERE
                                        MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                )
                            ) DCSLI 
                                ON MET.MA_EVENT_TYPE_ID = DCSLI.MA_EVENT_TYPE_ID 
                                AND DGC.STORE_LNBR::VARCHAR (25) = DCSLI.MA_EVENT_TYPE_VAR_VALUE 
                                AND GAD.GL_POSTING_DT BETWEEN DCSLI.START_EFF_DT AND DCSLI.END_EFF_DT 
                        LEFT JOIN
                            (
                                SELECT
                                    MA_EVENT_TYPE_ID,
                                    MA_EVENT_TYPE_VAR_VALUE,
                                    START_EFF_DT,
                                    END_EFF_DT 
                                FROM
                                    MA_EVENT_TYPE_VAR_CTRL 
                                WHERE
                                    MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_GL_ACCT_NBR_EXCL' 
                                    AND MA_EVENT_TYPE_ID IN (
                                        SELECT
                                            DISTINCT MA_EVENT_TYPE_ID 
                                        FROM
                                            MA_EVENT_TYPE_VAR_CTRL 
                                        WHERE
                                            MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                    )
                                ) DCGANE 
                                    ON MET.MA_EVENT_TYPE_ID = DCGANE.MA_EVENT_TYPE_ID 
                                    AND GAP.GL_ACCOUNT_NBR::VARCHAR (25) = DCGANE.MA_EVENT_TYPE_VAR_VALUE 
                                    AND GAD.GL_POSTING_DT BETWEEN DCGANE.START_EFF_DT AND DCGANE.END_EFF_DT 
                            LEFT JOIN
                                (
                                    SELECT
                                        MA_EVENT_TYPE_ID,
                                        MA_EVENT_TYPE_VAR_VALUE,
                                        START_EFF_DT,
                                        END_EFF_DT 
                                    FROM
                                        MA_EVENT_TYPE_VAR_CTRL 
                                    WHERE
                                        MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_GL_ACCT_NBR_INCL' 
                                        AND MA_EVENT_TYPE_ID IN (
                                            SELECT
                                                DISTINCT MA_EVENT_TYPE_ID 
                                            FROM
                                                MA_EVENT_TYPE_VAR_CTRL 
                                            WHERE
                                                MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                        )
                                    ) DCGANI 
                                        ON MET.MA_EVENT_TYPE_ID = DCGANI.MA_EVENT_TYPE_ID 
                                        AND GAP.GL_ACCOUNT_NBR::VARCHAR (25) = DCGANI.MA_EVENT_TYPE_VAR_VALUE 
                                        AND GAD.GL_POSTING_DT BETWEEN DCGANI.START_EFF_DT AND DCGANI.END_EFF_DT 
                                LEFT JOIN
                                    (
                                        SELECT
                                            MA_EVENT_TYPE_ID,
                                            CASE 
                                                WHEN LENGTH(SITE.STORE_NBR) = 1 THEN '0' 
                                                ELSE '' 
                                            END || SITE.STORE_NBR AS STORE_NBR,
                                            METVC.START_EFF_DT,
                                            METVC.END_EFF_DT 
                                        FROM
                                            MA_EVENT_TYPE_VAR_CTRL METVC 
                                        JOIN
                                            SITE_PROFILE SITE 
                                                ON METVC.MA_EVENT_TYPE_VAR_VALUE = SITE.LOCATION_TYPE_ID::VARCHAR (25) 
                                        WHERE
                                            METVC.MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_LOCATION_TYPE_ID_INCL' 
                                            AND LENGTH(SITE.STORE_NBR) <= 2 
                                            AND METVC.MA_EVENT_TYPE_ID IN (
                                                SELECT
                                                    DISTINCT MA_EVENT_TYPE_ID 
                                                FROM
                                                    MA_EVENT_TYPE_VAR_CTRL 
                                                WHERE
                                                    MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                            )
                                        ) LT 
                                            ON MET.MA_EVENT_TYPE_ID = LT.MA_EVENT_TYPE_ID 
                                            AND SUBSTR(GPCP.GL_PROFIT_CENTER_CD,
                                        1,
                                        7) = '0000003' 
                                        AND SUBSTR(GPCP.GL_PROFIT_CENTER_CD,
                                        9,
                                        2) = LT.STORE_NBR 
                                        AND GAD.GL_POSTING_DT BETWEEN LT.START_EFF_DT AND LT.END_EFF_DT 
                                    LEFT JOIN
                                        (
                                            SELECT
                                                MA_EVENT_TYPE_ID,
                                                MA_EVENT_TYPE_VAR_VALUE,
                                                METVC.START_EFF_DT,
                                                METVC.END_EFF_DT 
                                            FROM
                                                MA_EVENT_TYPE_VAR_CTRL METVC 
                                            WHERE
                                                METVC.MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_LOCATION_TYPE_ID_INCL' 
                                                AND METVC.MA_EVENT_TYPE_ID IN (
                                                    SELECT
                                                        DISTINCT MA_EVENT_TYPE_ID 
                                                    FROM
                                                        MA_EVENT_TYPE_VAR_CTRL 
                                                    WHERE
                                                        MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                                )
                                            ) LT2 
                                                ON MET.MA_EVENT_TYPE_ID = LT2.MA_EVENT_TYPE_ID 
                                                AND SITE.LOCATION_TYPE_ID::VARCHAR (25) = LT2.MA_EVENT_TYPE_VAR_VALUE 
                                                AND GAD.GL_POSTING_DT BETWEEN LT2.START_EFF_DT AND LT2.END_EFF_DT 
                                        LEFT JOIN
                                            (
                                                SELECT
                                                    MA_EVENT_TYPE_ID,
                                                    MA_EVENT_TYPE_VAR_VALUE,
                                                    START_EFF_DT,
                                                    END_EFF_DT 
                                                FROM
                                                    MA_EVENT_TYPE_VAR_CTRL 
                                                WHERE
                                                    MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_GL_PROFIT_CENTER_CD_INCL' 
                                                    AND MA_EVENT_TYPE_ID IN (
                                                        SELECT
                                                            DISTINCT MA_EVENT_TYPE_ID 
                                                        FROM
                                                            MA_EVENT_TYPE_VAR_CTRL 
                                                        WHERE
                                                            MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                                    )
                                                ) GPC 
                                                    ON MET.MA_EVENT_TYPE_ID = GPC.MA_EVENT_TYPE_ID 
                                                    AND GPCP.GL_PROFIT_CENTER_CD::VARCHAR (25) = GPC.MA_EVENT_TYPE_VAR_VALUE 
                                                    AND GAD.GL_POSTING_DT BETWEEN GPC.START_EFF_DT AND GPC.END_EFF_DT 
                                            WHERE
                                                DCGANE.MA_EVENT_TYPE_VAR_VALUE IS NULL 
                                                AND (
                                                    DCSLI.MA_EVENT_TYPE_VAR_VALUE IS NOT NULL 
                                                    OR DCGANI.MA_EVENT_TYPE_VAR_VALUE IS NOT NULL
                                                ) 
                                                AND (
                                                    LT.STORE_NBR IS NOT NULL 
                                                    OR LT2.MA_EVENT_TYPE_VAR_VALUE IS NOT NULL 
                                                    OR GPC.MA_EVENT_TYPE_VAR_VALUE IS NOT NULL
                                                ) 
                                            GROUP BY
                                                MET.MA_EVENT_TYPE_ID,
                                                GAD.FISCAL_MO
                                        ) C FULL 
                                    OUTER JOIN
                                        (
                                            SELECT
                                                DISTINCT M.MA_EVENT_TYPE_ID,
                                                D.FISCAL_MO,
                                                M.MA_EVENT_TYPE_VAR_VALUE::NUMERIC (12,
                                                2) ACT_DC_COST_USD 
                                            FROM
                                                MA_EVENT_TYPE_VAR_CTRL M 
                                            JOIN
                                                DAYS D 
                                                    ON D.DAY_DT BETWEEN M.START_EFF_DT AND M.END_EFF_DT 
                                            JOIN
                                                MA_FISCAL_MO_CTRL MFMC 
                                                    ON D.FISCAL_MO = MFMC.FISCAL_MO 
                                                    AND MFMC.RESTATE_DT = CURRENT_DATE 
                                            WHERE
                                                M.MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_GL_AMT_ADJ' 
                                                AND M.MA_EVENT_TYPE_ID IN (
                                                    SELECT
                                                        DISTINCT MA_EVENT_TYPE_ID 
                                                    FROM
                                                        MA_EVENT_TYPE_VAR_CTRL 
                                                    WHERE
                                                        MA_EVENT_TYPE_VAR_TYPE_CD = 'DC_COST_ADJ_MA_EVENT_TYPE_ID'
                                                )
                                            ) D 
                                                ON C.MA_EVENT_TYPE_ID = D.MA_EVENT_TYPE_ID 
                                                AND C.FISCAL_MO = D.FISCAL_MO
                                        ) B 
                                            ON A.REF_MA_EVENT_TYPE_ID = B.MA_EVENT_TYPE_ID 
                                            AND A.FISCAL_MO = B.FISCAL_MO
                                        ) A 
                                LEFT JOIN
                                    MA_DC_COST_ADJ_CTRL MDCAC 
                                        ON A.MA_EVENT_TYPE_ID = MDCAC.MA_EVENT_TYPE_ID 
                                        AND A.FISCAL_MO = MDCAC.FISCAL_MO 
                                WHERE
                                    INS_UPD_FLAG <> 'X'""")

df_11.createOrReplaceTempView("SQ_MA_DC_COST_ADJ_CTRL_11")

# COMMAND ----------
# DBTITLE 1, UPD_Strategy_12


df_12=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        EST_DC_COST_USD AS EST_DC_COST_USD,
        ACT_DC_COST_USD AS ACT_DC_COST_USD,
        DC_COST_ADJ_PCT AS DC_COST_ADJ_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_MA_DC_COST_ADJ_CTRL_11""")

df_12.createOrReplaceTempView("UPD_Strategy_12")

# COMMAND ----------
# DBTITLE 1, GL_ACTUAL_DETAIL_13


df_13=spark.sql("""
    SELECT
        FISCAL_YR AS FISCAL_YR,
        FISCAL_MO AS FISCAL_MO,
        GL_DOCUMENT_NBR AS GL_DOCUMENT_NBR,
        GL_COMPANY_CD AS GL_COMPANY_CD,
        GL_DOCUMENT_LINE_NBR AS GL_DOCUMENT_LINE_NBR,
        FISCAL_WK AS FISCAL_WK,
        WEEK_DT AS WEEK_DT,
        GL_DOCUMENT_DT AS GL_DOCUMENT_DT,
        GL_POSTING_DT AS GL_POSTING_DT,
        GL_DOCUMENT_ENTRY_DT AS GL_DOCUMENT_ENTRY_DT,
        GL_DOCUMENT_TYPE_CD AS GL_DOCUMENT_TYPE_CD,
        GL_REF_DOCUMENT_NBR AS GL_REF_DOCUMENT_NBR,
        GL_PROFIT_CENTER_GID AS GL_PROFIT_CENTER_GID,
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        GL_DEPARTMENT_CD AS GL_DEPARTMENT_CD,
        GL_DEBIT_CREDIT_IND AS GL_DEBIT_CREDIT_IND,
        GL_ACCOUNT_GID AS GL_ACCOUNT_GID,
        GL_BALANCE_SHEET_IND AS GL_BALANCE_SHEET_IND,
        GL_PL_SHEET_IND AS GL_PL_SHEET_IND,
        GL_SPLIT_LINE_ITEM_IND AS GL_SPLIT_LINE_ITEM_IND,
        GL_BUSINESS_TXN_TYPE_CD AS GL_BUSINESS_TXN_TYPE_CD,
        GL_REF_TXN_TYPE_CD AS GL_REF_TXN_TYPE_CD,
        GL_TXN_TYPE_CD AS GL_TXN_TYPE_CD,
        GL_COST_ELEMENT_CD AS GL_COST_ELEMENT_CD,
        GL_POSTING_KEY_GID AS GL_POSTING_KEY_GID,
        GL_CONTROLLING_AREA AS GL_CONTROLLING_AREA,
        GL_SEGMENT_CD AS GL_SEGMENT_CD,
        GL_PARTNER_PROFIT_CENTER_GID AS GL_PARTNER_PROFIT_CENTER_GID,
        GL_PARTNER_COMPANY_CD AS GL_PARTNER_COMPANY_CD,
        GL_PARTNER_SEGMENT_CD AS GL_PARTNER_SEGMENT_CD,
        VENDOR_ID AS VENDOR_ID,
        GL_ITEM_CATEGORY_CD AS GL_ITEM_CATEGORY_CD,
        GL_PURCH_DOC_NBR AS GL_PURCH_DOC_NBR,
        GL_DOC_CURRENCY_ID AS GL_DOC_CURRENCY_ID,
        GL_DOC_AMT AS GL_DOC_AMT,
        GL_LOC_CURRENCY_ID AS GL_LOC_CURRENCY_ID,
        GL_LOC_AMT AS GL_LOC_AMT,
        GL_GRP_CURRENCY_ID AS GL_GRP_CURRENCY_ID,
        GL_GRP_AMT AS GL_GRP_AMT,
        GL_QTY AS GL_QTY,
        GL_QTY_UOM_CD AS GL_QTY_UOM_CD,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        GL_USER_NAME AS GL_USER_NAME,
        GL_LOAD_TSTMP AS GL_LOAD_TSTMP,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_ACTUAL_DETAIL""")

df_13.createOrReplaceTempView("GL_ACTUAL_DETAIL_13")

# COMMAND ----------
# DBTITLE 1, DM_GL_COA_14


df_14=spark.sql("""
    SELECT
        GL_ACCT_NBR AS GL_ACCT_NBR,
        GL_ACCT_DESC AS GL_ACCT_DESC,
        STORE_LNBR AS STORE_LNBR,
        STORE_LNBR_DESC AS STORE_LNBR_DESC,
        SSG_LNBR AS SSG_LNBR,
        SSG_LNBR_DESC AS SSG_LNBR_DESC,
        CENTER AS CENTER,
        GL_CHART_OF_ACCOUNTS_CD AS GL_CHART_OF_ACCOUNTS_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DM_GL_COA""")

df_14.createOrReplaceTempView("DM_GL_COA_14")

# COMMAND ----------
# DBTITLE 1, MA_DC_COST_ADJ_CTRL


spark.sql("""INSERT INTO MA_DC_COST_ADJ_CTRL SELECT FISCAL_MO AS FISCAL_MO,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
EST_DC_COST_USD AS EST_DC_COST_USD,
ACT_DC_COST_USD AS ACT_DC_COST_USD,
DC_COST_ADJ_PCT AS DC_COST_ADJ_PCT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_Strategy_12""")