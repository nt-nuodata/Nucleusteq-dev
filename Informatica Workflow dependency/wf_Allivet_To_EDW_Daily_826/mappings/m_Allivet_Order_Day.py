# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY_0


df_0=spark.sql("""
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

df_0.createOrReplaceTempView("ALLIVET_ORDER_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_ORDER_DAY_1


df_1=spark.sql("""
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
        EXT_PRICEE AS EXT_PRICEE,
        ORDER_DETAIL_CREATED_DT AS ORDER_DETAIL_CREATED_DT,
        ORDER_DETAIL_MODIFIED_DT AS ORDER_DETAIL_MODIFIED_DT,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CD AS VET_CD,
        PET_CD AS PET_CD,
        ORDER_DETAIL_ONHOLD_FLAG AS ORDER_DETAIL_ONHOLD_FLAG,
        ONHOLD_TO_FILL_FLAG AS ONHOLD_TO_FILL_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_ORDER_DAY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_ORDER_DAY_1")

# COMMAND ----------
# DBTITLE 1, ALLIVET_DAILY_ORDER_PRE_2


df_2=spark.sql("""
    SELECT
        ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        ORDER_STATUS AS ORDER_STATUS,
        PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        SUB_TOTAL AS SUB_TOTAL,
        FREIGHT AS FREIGHT,
        TOTAL AS TOTAL,
        SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        SHIPPED_DATE AS SHIPPED_DATE,
        IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        IS_RISKORDER AS IS_RISKORDER,
        RISK_REASONS AS RISK_REASONS,
        ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        IS_SHIPHOLD AS IS_SHIPHOLD,
        SHIPHOLD_DATE AS SHIPHOLD_DATE,
        SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        ALLIVET_SKU AS ALLIVET_SKU,
        PETSMART_SKU AS PETSMART_SKU,
        ORDERED_QUANTITY AS ORDERED_QUANTITY,
        ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CODE AS VET_CODE,
        PET_CODE AS PET_CODE,
        IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_DAILY_ORDER_PRE""")

df_2.createOrReplaceTempView("ALLIVET_DAILY_ORDER_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_DAILY_ORDER_PRE_3


df_3=spark.sql("""
    SELECT
        ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        ORDER_STATUS AS ORDER_STATUS,
        PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        SUB_TOTAL AS SUB_TOTAL,
        FREIGHT AS FREIGHT,
        TOTAL AS TOTAL,
        SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        SHIPPED_DATE AS SHIPPED_DATE,
        IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        IS_RISKORDER AS IS_RISKORDER,
        RISK_REASONS AS RISK_REASONS,
        ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        IS_SHIPHOLD AS IS_SHIPHOLD,
        SHIPHOLD_DATE AS SHIPHOLD_DATE,
        SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        ALLIVET_SKU AS ALLIVET_SKU,
        PETSMART_SKU AS PETSMART_SKU,
        ORDERED_QUANTITY AS ORDERED_QUANTITY,
        ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CODE AS VET_CODE,
        PET_CODE AS PET_CODE,
        IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ALLIVET_DAILY_ORDER_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_DAILY_ORDER_PRE_3")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_4


df_4=spark.sql("""
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

df_4.createOrReplaceTempView("SKU_PROFILE_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PROFILE_5


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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SKU_PROFILE_5")

# COMMAND ----------
# DBTITLE 1, Jnr_Sku_Profile_6


df_6=spark.sql("""
    SELECT
        DETAIL.ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        DETAIL.ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        DETAIL.ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        DETAIL.PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        DETAIL.ORDER_STATUS AS ORDER_STATUS,
        DETAIL.PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        DETAIL.SUB_TOTAL AS SUB_TOTAL,
        DETAIL.FREIGHT AS FREIGHT,
        DETAIL.TOTAL AS TOTAL,
        DETAIL.SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        DETAIL.IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        DETAIL.IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        DETAIL.ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        DETAIL.ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        DETAIL.SHIPPED_DATE AS SHIPPED_DATE,
        DETAIL.IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        DETAIL.INTERNAL_NOTES AS INTERNAL_NOTES,
        DETAIL.PUBLIC_NOTES AS PUBLIC_NOTES,
        DETAIL.AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        DETAIL.ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        DETAIL.IS_RISKORDER AS IS_RISKORDER,
        DETAIL.RISK_REASONS AS RISK_REASONS,
        DETAIL.ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        DETAIL.IS_SHIPHOLD AS IS_SHIPHOLD,
        DETAIL.SHIPHOLD_DATE AS SHIPHOLD_DATE,
        DETAIL.SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        DETAIL.ALLIVET_SKU AS ALLIVET_SKU,
        DETAIL.PETSMART_SKU AS PETSMART_SKU,
        DETAIL.ORDERED_QUANTITY AS ORDERED_QUANTITY,
        DETAIL.ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        DETAIL.EXT_PRICE AS EXT_PRICE,
        DETAIL.ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        DETAIL.ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        DETAIL.HOW_TO_GET_RX AS HOW_TO_GET_RX,
        DETAIL.VET_CODE AS VET_CODE,
        DETAIL.PET_CODE AS PET_CODE,
        DETAIL.IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        DETAIL.IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        MASTER.SKU_NBR AS SKU_NBR,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_PROFILE_5 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_ALLIVET_DAILY_ORDER_PRE_3 DETAIL 
            ON MASTER.SKU_NBR = DETAIL.PETSMART_SKU""")

df_6.createOrReplaceTempView("Jnr_Sku_Profile_6")

# COMMAND ----------
# DBTITLE 1, Jnr_Order_Day_7


df_7=spark.sql("""
    SELECT
        DETAIL.ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        DETAIL.ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        DETAIL.ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        DETAIL.PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        DETAIL.ORDER_STATUS AS ORDER_STATUS,
        DETAIL.PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        DETAIL.SUB_TOTAL AS SUB_TOTAL,
        DETAIL.FREIGHT AS FREIGHT,
        DETAIL.TOTAL AS TOTAL,
        DETAIL.SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        DETAIL.IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        DETAIL.IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        DETAIL.ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        DETAIL.ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        DETAIL.SHIPPED_DATE AS SHIPPED_DATE,
        DETAIL.IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        DETAIL.INTERNAL_NOTES AS INTERNAL_NOTES,
        DETAIL.PUBLIC_NOTES AS PUBLIC_NOTES,
        DETAIL.AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        DETAIL.ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        DETAIL.IS_RISKORDER AS IS_RISKORDER,
        DETAIL.RISK_REASONS AS RISK_REASONS,
        DETAIL.ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        DETAIL.IS_SHIPHOLD AS IS_SHIPHOLD,
        DETAIL.SHIPHOLD_DATE AS SHIPHOLD_DATE,
        DETAIL.SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        DETAIL.ALLIVET_SKU AS ALLIVET_SKU,
        DETAIL.PETSMART_SKU AS PETSMART_SKU,
        DETAIL.ORDERED_QUANTITY AS ORDERED_QUANTITY,
        DETAIL.ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        DETAIL.EXT_PRICE AS EXT_PRICE,
        DETAIL.ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        DETAIL.ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        DETAIL.HOW_TO_GET_RX AS HOW_TO_GET_RX,
        DETAIL.VET_CODE AS VET_CODE,
        DETAIL.PET_CODE AS PET_CODE,
        DETAIL.IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        DETAIL.IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        MASTER.ALLIVET_ORDER_DT AS Lkp_ALLIVET_ORDER_DT,
        MASTER.ALLIVET_ORDER_NBR AS Lkp_ALLIVET_ORDER_NBR,
        MASTER.ALLIVET_ORDER_LN_NBR AS Lkp_ALLIVET_ORDER_LN_NBR,
        MASTER.ORDER_CREATED_DT AS Lkp_ORDER_CREATED_DT,
        MASTER.ORDER_MODIFIED_DT AS Lkp_ORDER_MODIFIED_DT,
        MASTER.ORDER_DETAIL_CREATED_DT AS Lkp_ORDER_DETAIL_CREATED_DT,
        MASTER.ORDER_DETAIL_MODIFIED_DT AS Lkp_ORDER_DETAIL_MODIFIED_DT,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ALLIVET_ORDER_DAY_1 MASTER 
    LEFT JOIN
        Jnr_Sku_Profile_6 DETAIL 
            ON MASTER.ALLIVET_ORDER_NBR = ALLIVET_ORDER_CODE 
            AND Lkp_ALLIVET_ORDER_LN_NBR = DETAIL.ALLIVET_ORDER_LINE_NUMBER""")

df_7.createOrReplaceTempView("Jnr_Order_Day_7")

# COMMAND ----------
# DBTITLE 1, Exp_Update_Flag_8


df_8=spark.sql("""SELECT ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
ORDER_STATUS AS ORDER_STATUS,
PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
SUB_TOTAL AS SUB_TOTAL,
FREIGHT AS FREIGHT,
TOTAL AS TOTAL,
SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
SHIPPED_DATE AS SHIPPED_DATE,
IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
INTERNAL_NOTES AS INTERNAL_NOTES,
PUBLIC_NOTES AS PUBLIC_NOTES,
AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
IS_RISKORDER AS IS_RISKORDER,
RISK_REASONS AS RISK_REASONS,
ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
IS_SHIPHOLD AS IS_SHIPHOLD,
SHIPHOLD_DATE AS SHIPHOLD_DATE,
SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
ALLIVET_SKU AS ALLIVET_SKU,
PETSMART_SKU AS PETSMART_SKU,
ORDERED_QUANTITY AS ORDERED_QUANTITY,
ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
EXT_PRICE AS EXT_PRICE,
ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
HOW_TO_GET_RX AS HOW_TO_GET_RX,
VET_CODE AS VET_CODE,
PET_CODE AS PET_CODE,
IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
LOAD_TSTMP AS LOAD_TSTMP,
PRODUCT_ID AS PRODUCT_ID,
SYSDATE AS UPDATE_TSTMP,
IIF(( ISNULL(Lkp_ALLIVET_ORDER_NBR) OR ISNULL(Lkp_ALLIVET_ORDER_LN_NBR) ),1,

IIF ( NOT ( ISNULL(Lkp_ALLIVET_ORDER_NBR) OR ISNULL(Lkp_ALLIVET_ORDER_LN_NBR) ) 
AND (
IIF( ISNULL(ORDER_CREATED_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'),ORDER_CREATED_DATE) <> IIF(ISNULL(Lkp_ORDER_CREATED_DT),TO_DATE('9999-12-31','YYYY-MM-DD'),Lkp_ORDER_CREATED_DT)
OR 
IIF( ISNULL(ORDER_MODIFIED_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'),ORDER_MODIFIED_DATE) <> IIF(ISNULL(Lkp_ORDER_MODIFIED_DT),TO_DATE('9999-12-31','YYYY-MM-DD'),Lkp_ORDER_MODIFIED_DT)

OR 
IIF( ISNULL(ORDER_DETAIL_CREATED_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'),ORDER_DETAIL_CREATED_DATE) <> IIF(ISNULL(Lkp_ORDER_DETAIL_CREATED_DT),TO_DATE('9999-12-31','YYYY-MM-DD'),Lkp_ORDER_DETAIL_CREATED_DT)

OR 
IIF( ISNULL(ORDER_DETAIL_MODIFIED_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'),ORDER_DETAIL_MODIFIED_DATE) <> IIF(ISNULL(Lkp_ORDER_DETAIL_MODIFIED_DT),TO_DATE('9999-12-31','YYYY-MM-DD'),Lkp_ORDER_DETAIL_MODIFIED_DT)),2,0)) AS UPDATE_FLAG,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM Jnr_Order_Day_7""")

df_8.createOrReplaceTempView("Exp_Update_Flag_8")

# COMMAND ----------
# DBTITLE 1, Fil_Update_Insert_9


df_9=spark.sql("""
    SELECT
        ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        ORDER_STATUS AS ORDER_STATUS,
        PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        SUB_TOTAL AS SUB_TOTAL,
        FREIGHT AS FREIGHT,
        TOTAL AS TOTAL,
        SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        SHIPPED_DATE AS SHIPPED_DATE,
        IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        IS_RISKORDER AS IS_RISKORDER,
        RISK_REASONS AS RISK_REASONS,
        ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        IS_SHIPHOLD AS IS_SHIPHOLD,
        SHIPHOLD_DATE AS SHIPHOLD_DATE,
        SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        ALLIVET_SKU AS ALLIVET_SKU,
        PETSMART_SKU AS PETSMART_SKU,
        ORDERED_QUANTITY AS ORDERED_QUANTITY,
        ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CODE AS VET_CODE,
        PET_CODE AS PET_CODE,
        IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        LOAD_TSTMP AS LOAD_TSTMP,
        PRODUCT_ID AS PRODUCT_ID,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        UPDATE_FLAG AS UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_Update_Flag_8 
    WHERE
        UPDATE_FLAG IN (
            1, 2
        )""")

df_9.createOrReplaceTempView("Fil_Update_Insert_9")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_10


df_10=spark.sql("""
    SELECT
        ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
        ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
        ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DATE,
        PETSMART_ORDER_DATE AS PETSMART_ORDER_DATE,
        ORDER_STATUS AS ORDER_STATUS,
        PETSMART_ORDER_CODE AS PETSMART_ORDER_CODE,
        SUB_TOTAL AS SUB_TOTAL,
        FREIGHT AS FREIGHT,
        TOTAL AS TOTAL,
        SHIPPING_METHOD_CODE AS SHIPPING_METHOD_CODE,
        IS_ORDER_VOIDED AS IS_ORDER_VOIDED,
        IS_ORDER_ONHOLD AS IS_ORDER_ONHOLD,
        ORDER_CREATED_DATE AS ORDER_CREATED_DATE,
        ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DATE,
        SHIPPED_DATE AS SHIPPED_DATE,
        IS_ORDER_SHIPPED AS IS_ORDER_SHIPPED,
        INTERNAL_NOTES AS INTERNAL_NOTES,
        PUBLIC_NOTES AS PUBLIC_NOTES,
        AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMOUNT,
        ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
        IS_RISKORDER AS IS_RISKORDER,
        RISK_REASONS AS RISK_REASONS,
        ORIGINAL_SHIPPING_METHOD_CODE AS ORIGINAL_SHIPPING_METHOD_CODE,
        IS_SHIPHOLD AS IS_SHIPHOLD,
        SHIPHOLD_DATE AS SHIPHOLD_DATE,
        SHIPRELEASED_DATE AS SHIPRELEASED_DATE,
        ALLIVET_SKU AS ALLIVET_SKU,
        PETSMART_SKU AS PETSMART_SKU,
        ORDERED_QUANTITY AS ORDERED_QUANTITY,
        ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
        EXT_PRICE AS EXT_PRICE,
        ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DATE,
        ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DATE,
        HOW_TO_GET_RX AS HOW_TO_GET_RX,
        VET_CODE AS VET_CODE,
        PET_CODE AS PET_CODE,
        IS_ORDER_DETAIL_ONHOLD AS IS_ORDER_DETAIL_ONHOLD,
        IS_ONHOLD_TOFILL AS IS_ONHOLD_TOFILL,
        LOAD_TSTMP AS LOAD_TSTMP,
        PRODUCT_ID AS PRODUCT_ID,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        UPDATE_FLAG AS UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_Update_Insert_9""")

df_10.createOrReplaceTempView("UPDTRANS_10")

# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY


spark.sql("""INSERT INTO ALLIVET_ORDER_DAY SELECT ALLIVET_ORDER_CODE AS ALLIVET_ORDER_NBR,
ALLIVET_ORDER_CODE AS ALLIVET_ORDER_NBR,
ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LN_NBR,
ALLIVET_ORDER_DATE AS ALLIVET_ORDER_DT,
PETSMART_ORDER_DATE AS PETSMART_ORDER_DT,
ORDER_STATUS AS ORDER_STATUS,
ORDER_STATUS AS ORDER_STATUS,
PRODUCT_ID AS PRODUCT_ID,
PETSMART_ORDER_CODE AS PETSMART_ORDER_NBR,
PETSMART_SKU AS PETSMART_SKU_NBR,
ALLIVET_SKU AS ALLIVET_SKU_NBR,
SUB_TOTAL AS SUB_TOTAL_AMT,
FREIGHT AS FREIGHT_COST,
TOTAL AS TOTAL_AMT,
SHIPPING_METHOD_CODE AS SHIP_METHOD_CD,
IS_ORDER_VOIDED AS ORDER_VOIDED_FLAG,
IS_ORDER_ONHOLD AS ORDER_ONHOLD_FLAG,
ORDER_CREATED_DATE AS ORDER_CREATED_DT,
ORDER_MODIFIED_DATE AS ORDER_MODIFIED_DT,
SHIPPED_DATE AS SHIPPED_DT,
IS_ORDER_SHIPPED AS ORDER_SHIPPED_FLAG,
INTERNAL_NOTES AS INTERNAL_NOTES,
PUBLIC_NOTES AS PUBLIC_NOTES,
AUTOSHIP_DISCOUNT_AMOUNT AS AUTOSHIP_DISCOUNT_AMT,
ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
IS_RISKORDER AS RISKORDER_FLAG,
RISK_REASONS AS RISK_REASON,
ORIGINAL_SHIPPING_METHOD_CODE AS ORIG_SHIP_METHOD_CD,
IS_SHIPHOLD AS SHIP_HOLD_FLAG,
SHIPHOLD_DATE AS SHIP_HOLD_DT,
SHIPRELEASED_DATE AS SHIP_RELEASE_DT,
ORDERED_QUANTITY AS ORDER_QTY,
ITEM_DESCRIPTION AS ITEM_DESC,
EXT_PRICE AS EXT_PRICE,
ORDER_DETAIL_CREATED_DATE AS ORDER_DETAIL_CREATED_DT,
ORDER_DETAIL_MODIFIED_DATE AS ORDER_DETAIL_MODIFIED_DT,
HOW_TO_GET_RX AS HOW_TO_GET_RX,
VET_CODE AS VET_CD,
PET_CODE AS PET_CD,
IS_ORDER_DETAIL_ONHOLD AS ORDER_DETAIL_ONHOLD_FLAG,
IS_ONHOLD_TOFILL AS ONHOLD_TO_FILL_FLAG,
UPDATE_TSTMP AS UPDATE_TSTMP,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPDTRANS_10""")