# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SAP_ATTRIBUTE_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATTRIBUTE""")

df_0.createOrReplaceTempView("SAP_ATTRIBUTE_0")

# COMMAND ----------
# DBTITLE 1, DM_DEPT_SEGMENTS_1


df_1=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        CONSUM_ID AS CONSUM_ID,
        CONSUM_DESC AS CONSUM_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DM_DEPT_SEGMENTS""")

df_1.createOrReplaceTempView("DM_DEPT_SEGMENTS_1")

# COMMAND ----------
# DBTITLE 1, SKU_CONTAINER_2


df_2=spark.sql("""
    SELECT
        CONTAINER_CD AS CONTAINER_CD,
        CONTAINER_DESC AS CONTAINER_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_CONTAINER""")

df_2.createOrReplaceTempView("SKU_CONTAINER_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_VALUE_3


df_3=spark.sql("""
    SELECT
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_VALUE""")

df_3.createOrReplaceTempView("SAP_ATT_VALUE_3")

# COMMAND ----------
# DBTITLE 1, OPT_PRODUCT_RELATIONSHIP_PRE_4


df_4=spark.sql("""
    SELECT
        PRODUCT_NUMBER AS PRODUCT_NUMBER,
        PRODUCT_DESC AS PRODUCT_DESC,
        SAP_DEMAND_GROUP AS SAP_DEMAND_GROUP,
        CUSTOMER_DEMAND_GROUP AS CUSTOMER_DEMAND_GROUP,
        SAP_PRODUCT_LINE AS SAP_PRODUCT_LINE,
        CUSTOMER_PRODUCT_LINE AS CUSTOMER_PRODUCT_LINE,
        SAP_PRICE_FAMILY AS SAP_PRICE_FAMILY,
        CUSTOMER_PRICE_FAMILY AS CUSTOMER_PRICE_FAMILY,
        EFFECTIVE_SIZE AS EFFECTIVE_SIZE,
        SELL_UNITS AS SELL_UNITS,
        UNIT_PRICE AS UNIT_PRICE,
        PRICE AS PRICE,
        COST AS COST,
        IS_NEW AS IS_NEW,
        ERROR_CODE AS ERROR_CODE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        OPT_PRODUCT_RELATIONSHIP_PRE""")

df_4.createOrReplaceTempView("OPT_PRODUCT_RELATIONSHIP_PRE_4")

# COMMAND ----------
# DBTITLE 1, SKU_FLAVOR_5


df_5=spark.sql("""
    SELECT
        FLAVOR_CD AS FLAVOR_CD,
        FLAVOR_DESC AS FLAVOR_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_FLAVOR""")

df_5.createOrReplaceTempView("SKU_FLAVOR_5")

# COMMAND ----------
# DBTITLE 1, BRAND_CLASSIFICATION_6


df_6=spark.sql("""
    SELECT
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        BRAND_CLASSIFICATION_NAME AS BRAND_CLASSIFICATION_NAME,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BRAND_CLASSIFICATION""")

df_6.createOrReplaceTempView("BRAND_CLASSIFICATION_6")

# COMMAND ----------
# DBTITLE 1, SAP_T6WP1T_PRE_7


df_7=spark.sql("""
    SELECT
        PROCUREMENT_RULE_CD AS PROCUREMENT_RULE_CD,
        PROCUREMENT_RULE_DESC AS PROCUREMENT_RULE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_T6WP1T_PRE""")

df_7.createOrReplaceTempView("SAP_T6WP1T_PRE_7")

# COMMAND ----------
# DBTITLE 1, GL_CATEGORY_8


df_8=spark.sql("""
    SELECT
        GL_CATEGORY_CD AS GL_CATEGORY_CD,
        GL_CATEGORY_DESC AS GL_CATEGORY_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_CATEGORY""")

df_8.createOrReplaceTempView("GL_CATEGORY_8")

# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT_9


df_9=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        HAZ_FLAG AS HAZ_FLAG,
        AEROSOL_FLAG AS AEROSOL_FLAG,
        ADD_DT AS ADD_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT""")

df_9.createOrReplaceTempView("SKU_HAZMAT_9")

# COMMAND ----------
# DBTITLE 1, OWNBRAND_10


df_10=spark.sql("""
    SELECT
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        OWNBRAND_DESC AS OWNBRAND_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        OWNBRAND""")

df_10.createOrReplaceTempView("OWNBRAND_10")

# COMMAND ----------
# DBTITLE 1, ZDISCO_MKDN_SCHED_11


df_11=spark.sql("""
    SELECT
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_MKDN_SCHED_DESC AS ZDISCO_MKDN_SCHED_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZDISCO_MKDN_SCHED""")

df_11.createOrReplaceTempView("ZDISCO_MKDN_SCHED_11")

# COMMAND ----------
# DBTITLE 1, SAP_ZTH_WEB_WIP_PRE_12


df_12=spark.sql("""
    SELECT
        RECORD_ID AS RECORD_ID,
        MATKL AS MATKL,
        WSTAW AS WSTAW,
        POG_REPLACE AS POG_REPLACE,
        DISC_SKU_NBR AS DISC_SKU_NBR,
        RTV_DISP AS RTV_DISP,
        SELL_THROUGH AS SELL_THROUGH,
        PRES_MGR AS PRES_MGR,
        NOTIFY_PRICE AS NOTIFY_PRICE,
        PRICE_MGR AS PRICE_MGR,
        KHOP AS KHOP,
        COLOR AS COLOR,
        SIZE1 AS SIZE1,
        FLAVOR AS FLAVOR,
        RTV AS RTV,
        DISCIPLINE AS DISCIPLINE,
        STATUS_CODE AS STATUS_CODE,
        PLAN_GROUP AS PLAN_GROUP,
        STATELINETACK AS STATELINETACK,
        DUE_IN_STORE AS DUE_IN_STORE,
        QTY_FOR_DC4 AS QTY_FOR_DC4,
        RDPRF AS RDPRF,
        APU_QTY AS APU_QTY,
        USITEM AS USITEM,
        CAITEM AS CAITEM,
        US_CA_DIR_ITEM AS US_CA_DIR_ITEM,
        INLINE AS INLINE,
        DPR_SKU_NBR AS DPR_SKU_NBR,
        DPR_PLUS AS DPR_PLUS,
        DPR_PERC AS DPR_PERC,
        DPR_MIN_QTY AS DPR_MIN_QTY,
        OTB AS OTB,
        BUYER_SUGGEST AS BUYER_SUGGEST,
        BUYER_A AS BUYER_A,
        BUYER_B AS BUYER_B,
        BUYER_C AS BUYER_C,
        BUYER_D AS BUYER_D,
        BUYER_E AS BUYER_E,
        BUYER_F AS BUYER_F,
        DC8 AS DC8,
        DC9 AS DC9,
        DC10 AS DC10,
        DC12 AS DC12,
        S2920 AS S2920,
        ALLOC AS ALLOC,
        PRODUCT_CODE AS PRODUCT_CODE,
        BASE_NUMBER AS BASE_NUMBER,
        BASE_DESC AS BASE_DESC,
        SLT_CAT_CODE AS SLT_CAT_CODE,
        SUB_PROD_CODE AS SUB_PROD_CODE,
        SADDLE AS SADDLE,
        SIZE_CHART_NUM AS SIZE_CHART_NUM,
        CASE_PACK_QTY AS CASE_PACK_QTY,
        HAZ_MAT AS HAZ_MAT,
        AEROSOL AS AEROSOL,
        ROLL AS ROLL,
        OVER_SIZE AS OVER_SIZE,
        PSM_LABEL AS PSM_LABEL,
        OWNS AS OWNS,
        BASE_GEN AS BASE_GEN,
        DCLR AS DCLR,
        DSZE AS DSZE,
        DBRD AS DBRD,
        DFRT AS DFRT,
        STOR AS STOR,
        WEB AS WEB,
        CATL AS CATL,
        DTCD AS DTCD,
        DLBL AS DLBL,
        PROJECT_ID AS PROJECT_ID,
        CREATED_BY AS CREATED_BY,
        CREATED_TSTMP AS CREATED_TSTMP,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_TSTMP AS LAST_CHANGED_TSTMP,
        DC36 AS DC36,
        S2910 AS S2910,
        S2930 AS S2930,
        AVG_SALES AS AVG_SALES,
        SUBC AS SUBC,
        SPID AS SPID,
        DC38 AS DC38,
        AVG_CATWEB AS AVG_CATWEB,
        INIT_BUY AS INIT_BUY,
        COPY_SKU_NBR AS COPY_SKU_NBR,
        COPY_SKU_PCT AS COPY_SKU_PCT,
        ALL_STORES AS ALL_STORES,
        LIM_STORES AS LIM_STORES,
        WEB_STYLE AS WEB_STYLE,
        WEB_STYLE_TEXT AS WEB_STYLE_TEXT,
        WEB_HAZD AS WEB_HAZD,
        DC41 AS DC41,
        DC14 AS DC14,
        DC16 AS DC16,
        DC18 AS DC18,
        EXP_DAYS AS EXP_DAYS,
        MHDRZ AS MHDRZ,
        DC39 AS DC39,
        DC40 AS DC40,
        DC42 AS DC42,
        COPY_SKU_PCT_SIGN AS COPY_SKU_PCT_SIGN,
        PRITEM AS PRITEM,
        DC43 AS DC43,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ZTH_WEB_WIP_PRE""")

df_12.createOrReplaceTempView("SAP_ZTH_WEB_WIP_PRE_12")

# COMMAND ----------
# DBTITLE 1, ZDISCO_SCHED_TYPE_13


df_13=spark.sql("""
    SELECT
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_SCHED_TYPE_CD AS ZDISCO_SCHED_TYPE_CD,
        ZDISCO_SCHED_TYPE_DESC AS ZDISCO_SCHED_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZDISCO_SCHED_TYPE""")

df_13.createOrReplaceTempView("ZDISCO_SCHED_TYPE_13")

# COMMAND ----------
# DBTITLE 1, SIGN_TYPE_14


df_14=spark.sql("""
    SELECT
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        SIGN_TYPE_DESC AS SIGN_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SIGN_TYPE""")

df_14.createOrReplaceTempView("SIGN_TYPE_14")

# COMMAND ----------
# DBTITLE 1, IMPORT_15


df_15=spark.sql("""
    SELECT
        IMPORT_FLAG AS IMPORT_FLAG,
        IMPORT_DESC AS IMPORT_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        IMPORT""")

df_15.createOrReplaceTempView("IMPORT_15")

# COMMAND ----------
# DBTITLE 1, ARTMAS_PRE_16


df_16=spark.sql("""
    SELECT
        ARTICLE_NBR AS ARTICLE_NBR,
        ARTICLE_DESC AS ARTICLE_DESC,
        ALTERNATE_DESC AS ALTERNATE_DESC,
        ARTICLE_TYPE_CD AS ARTICLE_TYPE_CD,
        MERCH_CATEGORY_CD AS MERCH_CATEGORY_CD,
        ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
        ARTICLE_STATUS_CD AS ARTICLE_STATUS_CD,
        PRIMARY_VENDOR_CD AS PRIMARY_VENDOR_CD,
        BRAND_CD AS BRAND_CD,
        PROC_RULE AS PROC_RULE,
        FLAVOR_CD AS FLAVOR_CD,
        COLOR_CD AS COLOR_CD,
        SIZE_CD AS SIZE_CD,
        RTV_CD AS RTV_CD,
        CREATE_DT AS CREATE_DT,
        CREATED_BY AS CREATED_BY,
        UPDATE_DT AS UPDATE_DT,
        UPDATED_BY AS UPDATED_BY,
        BASE_UOM_CD AS BASE_UOM_CD,
        BASE_UOM_ISO_CD AS BASE_UOM_ISO_CD,
        DOC_NBR AS DOC_NBR,
        DOC_SHEET_CNT AS DOC_SHEET_CNT,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        CONTAINER_REQMT_CD AS CONTAINER_REQMT_CD,
        TRANSPORT_GROUP_CD AS TRANSPORT_GROUP_CD,
        DIVISION_CD AS DIVISION_CD,
        GR_GI_SLIP_PRINT_CNT AS GR_GI_SLIP_PRINT_CNT,
        SUPPLY_SOURCE_CD AS SUPPLY_SOURCE_CD,
        WEIGHT_ALLOWED_PKG_AMT AS WEIGHT_ALLOWED_PKG_AMT,
        VOLUME_ALLOWED_PKG_AMT AS VOLUME_ALLOWED_PKG_AMT,
        WEIGHT_TOLERANCE_AMT AS WEIGHT_TOLERANCE_AMT,
        VOLUME_TOLERANCE_AMT AS VOLUME_TOLERANCE_AMT,
        VARIABLE_ORDER_UNIT_FLAG AS VARIABLE_ORDER_UNIT_FLAG,
        VOLUME_FILL_AMT AS VOLUME_FILL_AMT,
        STACKING_FACTOR_AMT AS STACKING_FACTOR_AMT,
        SHELF_LIFE_REM_CNT AS SHELF_LIFE_REM_CNT,
        SHELF_LIFE_TOTAL_CNT AS SHELF_LIFE_TOTAL_CNT,
        STORAGE_PCT AS STORAGE_PCT,
        VALID_FROM_DT AS VALID_FROM_DT,
        DELETE_DT AS DELETE_DT,
        XSITE_STATUS_CD AS XSITE_STATUS_CD,
        XSITE_VALID_FROM_DT AS XSITE_VALID_FROM_DT,
        XDIST_VALID_FROM_DT AS XDIST_VALID_FROM_DT,
        TAX_CLASS_CD AS TAX_CLASS_CD,
        CONTENT_UNIT_CD AS CONTENT_UNIT_CD,
        NET_CONTENTS_AMT AS NET_CONTENTS_AMT,
        CONTENT_METRIC_UNIT_CD AS CONTENT_METRIC_UNIT_CD,
        NET_CONTENTS_METRIC_AMT AS NET_CONTENTS_METRIC_AMT,
        COMP_PRICE_UNIT_AMT AS COMP_PRICE_UNIT_AMT,
        GROSS_CONTENTS_AMT AS GROSS_CONTENTS_AMT,
        ITEM_CATEGORY AS ITEM_CATEGORY,
        FIBER_SHARE1_PCT AS FIBER_SHARE1_PCT,
        FIBER_SHARE2_PCT AS FIBER_SHARE2_PCT,
        FIBER_SHARE3_PCT AS FIBER_SHARE3_PCT,
        FIBER_SHARE4_PCT AS FIBER_SHARE4_PCT,
        FIBER_SHARE5_PCT AS FIBER_SHARE5_PCT,
        TEMP_SKU AS TEMP_SKU,
        COPY_SKU AS COPY_SKU,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        BASIC_MATERIAL AS BASIC_MATERIAL,
        RX_FLAG AS RX_FLAG,
        SEASONALITY AS SEASONALITY,
        IDOC_NUMBER AS IDOC_NUMBER,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ARTMAS_PRE""")

df_16.createOrReplaceTempView("ARTMAS_PRE_16")

# COMMAND ----------
# DBTITLE 1, MERCHCAT_ORG_17


df_17=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD,
        MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC,
        CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
        CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
        CATEGORY_REPLENISHMENT_MGR_ID AS CATEGORY_REPLENISHMENT_MGR_ID,
        CATEGORY_REPLENISHMENT_MGR_NM AS CATEGORY_REPLENISHMENT_MGR_NM,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MERCHCAT_ORG""")

df_17.createOrReplaceTempView("MERCHCAT_ORG_17")

# COMMAND ----------
# DBTITLE 1, SAP_ZTH_WEB_RECORD_PRE_18


df_18=spark.sql("""
    SELECT
        RECORD_ID AS RECORD_ID,
        AVAIL_ON_WEB AS AVAIL_ON_WEB,
        LIFNR AS LIFNR,
        IDNLF AS IDNLF,
        BRAND AS BRAND,
        BUYER AS BUYER,
        PUR_GRP AS PUR_GRP,
        ART_TYP AS ART_TYP,
        ART_CAT AS ART_CAT,
        SHIPPER AS SHIPPER,
        CORP_BRAND AS CORP_BRAND,
        PACK_TYPE AS PACK_TYPE,
        COUNTRY_OF_ORIGI AS COUNTRY_OF_ORIGI,
        CODE_DATE_IND AS CODE_DATE_IND,
        EXP_MONTH AS EXP_MONTH,
        US_LOC AS US_LOC,
        SITE_SPECIFIC_C AS SITE_SPECIFIC_C,
        COST AS COST,
        CURR_UNIT AS CURR_UNIT,
        COST_PER AS COST_PER,
        ORDER_UNIT AS ORDER_UNIT,
        LEAD_TIME AS LEAD_TIME,
        DIRECT_STORE AS DIRECT_STORE,
        TAX_CLASS AS TAX_CLASS,
        SADDLE AS SADDLE,
        HAZARDOUS AS HAZARDOUS,
        AEROSOL AS AEROSOL,
        DATE_AVAILABLE AS DATE_AVAILABLE,
        IMPORT AS IMPORT,
        NET_CONTENTS AS NET_CONTENTS,
        CONTENT_UNIT AS CONTENT_UNIT,
        SP_HANDLE AS SP_HANDLE,
        LEGAL_IN_CA AS LEGAL_IN_CA,
        CA_STUFFED AS CA_STUFFED,
        INGREDIENTS AS INGREDIENTS,
        EPA AS EPA,
        ALL_STATES AS ALL_STATES,
        SKU_NBR AS SKU_NBR,
        EMAIL AS EMAIL,
        CHANGE_REC AS CHANGE_REC,
        CHANGE_FLAGS AS CHANGE_FLAGS,
        INNER_PACK_TYPE AS INNER_PACK_TYPE,
        ARTILCE_SUB_CAT AS ARTILCE_SUB_CAT,
        DATE_AVAIL AS DATE_AVAIL,
        CREATED_BY AS CREATED_BY,
        CREATED_TSTMP AS CREATED_TSTMP,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_TSTMP AS LAST_CHANGED_TSTMP,
        DATBI AS DATBI,
        SUB_DATE AS SUB_DATE,
        RTV AS RTV,
        NESTLE_EDI AS NESTLE_EDI,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ZTH_WEB_RECORD_PRE""")

df_18.createOrReplaceTempView("SAP_ZTH_WEB_RECORD_PRE_18")

# COMMAND ----------
# DBTITLE 1, BRAND_19


df_19=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BRAND""")

df_19.createOrReplaceTempView("BRAND_19")

# COMMAND ----------
# DBTITLE 1, MERCHDEPT_ORG_20


df_20=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        BUS_UNIT_ID AS BUS_UNIT_ID,
        BUS_UNIT_DESC AS BUS_UNIT_DESC,
        BUYER_ID AS BUYER_ID,
        BUYER_NM AS BUYER_NM,
        CA_BUYER_ID AS CA_BUYER_ID,
        CA_BUYER_NM AS CA_BUYER_NM,
        CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
        CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
        CA_MANAGED_FLG AS CA_MANAGED_FLG,
        DIRECTOR_ID AS DIRECTOR_ID,
        DIRECTOR_NM AS DIRECTOR_NM,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        PRICING_ROLE_ID AS PRICING_ROLE_ID,
        PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SVP_ID AS SVP_ID,
        SVP_NM AS SVP_NM,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        VP_NM AS VP_NM,
        CA_VP_ID AS CA_VP_ID,
        CA_VP_NM AS CA_VP_NM,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MERCHDEPT_ORG""")

df_20.createOrReplaceTempView("MERCHDEPT_ORG_20")

# COMMAND ----------
# DBTITLE 1, OPT_ASSOCIATED_PRODUCT_21


df_21=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        RELATIONSHIP AS RELATIONSHIP,
        METRIC AS METRIC,
        DIFF AS DIFF,
        ASSOC_PROD AS ASSOC_PROD,
        DELETE_OPTION AS DELETE_OPTION,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        OPT_ASSOCIATED_PRODUCT""")

df_21.createOrReplaceTempView("OPT_ASSOCIATED_PRODUCT_21")

# COMMAND ----------
# DBTITLE 1, VALUATION_CLASS_22


df_22=spark.sql("""
    SELECT
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        VALUATION_CLASS_DESC AS VALUATION_CLASS_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VALUATION_CLASS""")

df_22.createOrReplaceTempView("VALUATION_CLASS_22")

# COMMAND ----------
# DBTITLE 1, ARTICLE_CATEGORY_23


df_23=spark.sql("""
    SELECT
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
        ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_CATEGORY""")

df_23.createOrReplaceTempView("ARTICLE_CATEGORY_23")

# COMMAND ----------
# DBTITLE 1, SKU_STATUS_24


df_24=spark.sql("""
    SELECT
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_STATUS""")

df_24.createOrReplaceTempView("SKU_STATUS_24")

# COMMAND ----------
# DBTITLE 1, BUYER_25


df_25=spark.sql("""
    SELECT
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BUYER""")

df_25.createOrReplaceTempView("BUYER_25")

# COMMAND ----------
# DBTITLE 1, HTS_26


df_26=spark.sql("""
    SELECT
        HTS_CODE_ID AS HTS_CODE_ID,
        HTS_CODE_DESC AS HTS_CODE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        HTS""")

df_26.createOrReplaceTempView("HTS_26")

# COMMAND ----------
# DBTITLE 1, COUNTRY_27


df_27=spark.sql("""
    SELECT
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        COUNTRY""")

df_27.createOrReplaceTempView("COUNTRY_27")

# COMMAND ----------
# DBTITLE 1, SAP_CLASS_28


df_28=spark.sql("""
    SELECT
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_CLASS""")

df_28.createOrReplaceTempView("SAP_CLASS_28")

# COMMAND ----------
# DBTITLE 1, SAP_CATEGORY_29


df_29=spark.sql("""
    SELECT
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        GL_CATEGORY_CD AS GL_CATEGORY_CD,
        SAP_PRICING_CATEGORY_ID AS SAP_PRICING_CATEGORY_ID,
        UPD_TSTMP AS UPD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_CATEGORY""")

df_29.createOrReplaceTempView("SAP_CATEGORY_29")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_30


df_30=spark.sql("""
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

df_30.createOrReplaceTempView("SKU_PROFILE_30")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_31


df_31=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_NBR AS VENDOR_NBR,
        LOCATION_ID AS LOCATION_ID,
        SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        PURCHASE_BLOCK AS PURCHASE_BLOCK,
        POSTING_BLOCK AS POSTING_BLOCK,
        DELETION_FLAG AS DELETION_FLAG,
        VIP_CD AS VIP_CD,
        INACTIVE_FLAG AS INACTIVE_FLAG,
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        INCO_TERM_CD AS INCO_TERM_CD,
        ADDRESS AS ADDRESS,
        CITY AS CITY,
        STATE AS STATE,
        COUNTRY_CD AS COUNTRY_CD,
        ZIP AS ZIP,
        CONTACT AS CONTACT,
        CONTACT_PHONE AS CONTACT_PHONE,
        PHONE AS PHONE,
        PHONE_EXT AS PHONE_EXT,
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        LATITUDE AS LATITUDE,
        LONGITUDE AS LONGITUDE,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        ADD_DT AS ADD_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE""")

df_31.createOrReplaceTempView("VENDOR_PROFILE_31")

# COMMAND ----------
# DBTITLE 1, TAX_CLASS_32


df_32=spark.sql("""
    SELECT
        TAX_CLASS_ID AS TAX_CLASS_ID,
        TAX_CLASS_DESC AS TAX_CLASS_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TAX_CLASS""")

df_32.createOrReplaceTempView("TAX_CLASS_32")

# COMMAND ----------
# DBTITLE 1, SAP_DIVISION_33


df_33=spark.sql("""
    SELECT
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        MERCH_SVP_ID AS MERCH_SVP_ID,
        MERCH_VP_ID AS MERCH_VP_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_DIVISION""")

df_33.createOrReplaceTempView("SAP_DIVISION_33")

# COMMAND ----------
# DBTITLE 1, SAP_DEPT_34


df_34=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
        BUYER_ID AS BUYER_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_DEPT""")

df_34.createOrReplaceTempView("SAP_DEPT_34")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP_35


df_35=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        DVL_MGR_ID AS DVL_MGR_ID,
        REPLEN_MGR_ID AS REPLEN_MGR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP""")

df_35.createOrReplaceTempView("PURCH_GROUP_35")

# COMMAND ----------
# DBTITLE 1, PRIMARY_VENDOR_36


df_36=spark.sql("""
    SELECT
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PRIMARY_VENDOR""")

df_36.createOrReplaceTempView("PRIMARY_VENDOR_36")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE_37


df_37=spark.sql("""
    SELECT
        sp.product_id,
        sp.sku_nbr,
        sp.sku_type,
        sp.primary_upc_id,
        sp.status_id,
        rtrim(ss.status_name) AS status_name,
        sp.subs_hist_flag,
        sp.subs_curr_flag,
        rtrim(sp.sku_desc) sku_desc,
        rtrim(sp.alt_desc) alt_desc,
        sp.sap_category_id,
        rtrim(sc.sap_category_desc) sap_category_desc,
        sc.gl_category_cd,
        rtrim(gc.gl_category_desc) gl_category_desc,
        mo.bus_unit_id,
        rtrim(mo.bus_unit_desc) bus_unit_desc,
        mo.svp_id,
        rtrim(mo.svp_nm) svp_desc,
        mo.vp_id,
        rtrim(mo.vp_nm) vp_nm,
        rtrim(mo.vp_desc) vp_desc,
        mo.buyer_id AS category_buyer_id,
        rtrim(mo.buyer_nm) AS category_buyer_nm,
        mo.ca_buyer_id AS ca_buyer_id,
        rtrim(mo.ca_buyer_nm) AS ca_buyer_nm,
        mo.director_id,
        rtrim(mo.director_nm) AS director_desc,
        mo.ca_director_id,
        rtrim(mo.ca_director_nm) AS ca_director_desc,
        stp.procurement_rule_cd,
        stp.procurement_rule_desc,
        sp.sap_class_id,
        rtrim(scl.sap_class_desc) sap_class_desc,
        sp.sap_dept_id,
        rtrim(sd.sap_dept_desc) sap_dept_desc,
        mo.group_id,
        rtrim(mo.group_desc) consum_desc,
        mo.segment_id,
        rtrim(mo.segment_desc) segment_desc,
        sp.sap_division_id,
        rtrim(sdiv.sap_division_desc) AS sap_division_desc,
        sp.primary_vendor_id,
        rtrim(pv.primary_vendor_name) AS primary_vendor_name,
        vp.purch_group_id,
        rtrim(pg.purch_group_name) AS purch_group_name,
        vp.parent_vendor_id,
        rtrim(vp.parent_vendor_name) AS parent_vendor_name,
        sp.country_cd,
        rtrim(c.country_name) AS country_name,
        sp.import_flag,
        rtrim(i.import_desc) AS import_desc,
        sp.hts_code_id,
        rtrim(h.hts_code_desc) AS hts_code_desc,
        sp.contents,
        rtrim(sp.contents_units) AS contents_units,
        sp.weight_net_amt,
        sp.weight_uom_cd,
        rtrim(sp.size_desc) AS size_desc,
        sp.bum_qty,
        sp.uom_cd,
        sp.unit_numerator,
        sp.unit_denominator,
        sp.buyer_id,
        rtrim(b.buyer_name) AS buyer_name,
        sp.purch_cost_amt,
        sp.nat_price_us_amt,
        sp.tax_class_id,
        rtrim(tc.tax_class_desc) AS tax_class_desc,
        sp.valuation_class_cd,
        rtrim(vc.valuation_class_desc) AS valuation_class_desc,
        sp.brand_cd,
        br.brand_name,
        sp.brand_classification_id,
        bc.brand_classification_name,
        sp.ownbrand_flag,
        rtrim(o.ownbrand_desc) AS ownbrand_desc,
        sp.stateline_flag,
        sp.sign_type_cd,
        rtrim(st.sign_type_desc) AS sign_type_desc,
        sp.old_article_nbr,
        sp.vendor_article_nbr,
        sp.init_mkdn_dt,
        sp.disc_start_dt,
        sp.add_dt,
        sp.delete_dt,
        sp.first_sale_dt,
        sp.last_sale_dt,
        sp.first_inv_dt,
        sp.last_inv_dt,
        sp.base_nbr,
        sp.bp_color_id,
        sp.bp_size_id,
        sp.bp_breed_id,
        sp.bp_item_concatenated,
        sp.canadian_hts_cd,
        sp.nat_price_ca_amt,
        sp.nat_price_pr_amt,
        sp.rtv_dept_cd,
        rtrim(sh.rtv_desc) AS rtv_desc,
        sh.haz_flag,
        sh.aerosol_flag,
        sp.gl_acct_nbr,
        sp.article_category_id,
        rtrim(ac.article_category_desc),
        ac.article_category_cd,
        sp.component_flag,
        sp.zdisco_sched_type_id,
        rtrim(zst.zdisco_sched_type_desc) AS zdisco_sched_type_desc,
        sp.zdisco_mkdn_sched_id,
        rtrim(zms.zdisco_mkdn_sched_desc) AS zdisco_mkdn_sched_desc,
        sp.zdisco_pid_dt,
        sp.zdisco_start_dt,
        sp.zdisco_init_mkdn_dt,
        sp.zdisco_dc_dt,
        sp.zdisco_str_dt,
        sp.zdisco_str_ownrshp_dt,
        sp.zdisco_str_wrt_off_dt,
        NULL AS sap_demand_group,
        NULL AS sap_product_line,
        NULL AS sap_price_family,
        NULL AS effective_size,
        NULL AS sell_units,
        NULL AS unit_price,
        NULL AS price,
        NULL AS cost,
        NULL AS is_new,
        NULL AS error_code,
        NULL AS relationship,
        NULL AS metric,
        NULL AS diff,
        NULL AS assoc_prod,
        NULL AS delete_option,
        znewitem.disc_sku_nbr,
        znewitem.avg_sales,
        znewitem.copy_sku_nbr,
        znewitem.copy_sku_pct,
        stp.procurement_rule_cd,
        stp.procurement_rule_desc,
        IFNULL(mco.CATEGORY_ANALYST_ID,
        999) AS CATEGORY_ANALYST_ID,
        IFNULL(mco.CATEGORY_ANALYST_NM,
        'Not Defined') AS CATEGORY_ANALYST_NM,
        IFNULL(mco.CATEGORY_REPLENISHMENT_MGR_ID,
        999) AS CATEGORY_REPLENISHMENT_MGR_ID,
        IFNULL(mco.CATEGORY_REPLENISHMENT_MGR_NM,
        'Not Defined') AS CATEGORY_REPLENISHMENT_MGR_NM,
        artmas.created_by AS created_by,
        artmas.shelf_life_rem_cnt AS shelf_life_rem_cnt,
        artmas.temp_sku AS temp_sku,
        artmas.net_contents_metric_amt,
        artmas.content_metric_unit_cd,
        skf.flavor_cd,
        skf.flavor_desc,
        artmas.basic_material,
        skc.container_cd,
        skc.container_desc,
        artmas.rx_flag,
        sp.update_dt,
        sp.load_dt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        sku_profile sp 
    LEFT OUTER JOIN
        sku_status ss 
            ON (
                sp.status_id = ss.status_id
            ) 
    LEFT OUTER JOIN
        sap_category sc 
            ON (
                sp.sap_category_id = sc.sap_category_id
            ) 
    LEFT OUTER JOIN
        sap_class scl 
            ON (
                sp.sap_class_id = scl.sap_class_id
            ) 
    LEFT OUTER JOIN
        sap_dept sd 
            ON (
                sp.sap_dept_id = sd.sap_dept_id
            ) 
    LEFT OUTER JOIN
        sap_division sdiv 
            ON (
                sp.sap_division_id = sdiv.sap_division_id
            ) 
    LEFT OUTER JOIN
        primary_vendor pv 
            ON (
                sp.primary_vendor_id = pv.primary_vendor_id
            ) 
    LEFT OUTER JOIN
        merchdept_org mo 
            ON (
                sp.sap_dept_id = mo.sap_dept_id
            ) 
    LEFT OUTER JOIN
        valuation_class vc 
            ON (
                sp.valuation_class_cd = vc.valuation_class_cd
            ) 
    LEFT OUTER JOIN
        import i 
            ON (
                sp.import_flag = i.import_flag
            ) 
    LEFT OUTER JOIN
        sku_hazmat sh 
            ON (
                sp.rtv_dept_cd = sh.rtv_dept_cd
            ) 
    LEFT OUTER JOIN
        sign_type st 
            ON (
                sp.sign_type_cd = st.sign_type_cd
            ) 
    LEFT OUTER JOIN
        zdisco_mkdn_sched zms 
            ON (
                sp.zdisco_mkdn_sched_id = zms.zdisco_mkdn_sched_id
            ) 
    LEFT OUTER JOIN
        zdisco_sched_type zst 
            ON (
                sp.zdisco_sched_type_id = zst.zdisco_sched_type_id
            ) 
    LEFT OUTER JOIN
        gl_category gc 
            ON (
                sc.gl_category_cd = gc.gl_category_cd
            ) 
    LEFT OUTER JOIN
        vendor_profile vp 
            ON (
                sp.primary_vendor_id = vp.vendor_id
            ) 
    LEFT OUTER JOIN
        purch_group pg 
            ON (
                vp.purch_group_id = pg.purch_group_id
            ) 
    LEFT OUTER JOIN
        hts h 
            ON (
                sp.hts_code_id = h.hts_code_id
            ) 
    LEFT OUTER JOIN
        ownbrand o 
            ON (
                sp.ownbrand_flag = o.ownbrand_flag
            ) 
    LEFT OUTER JOIN
        country c 
            ON (
                sp.country_cd = c.country_cd
            ) 
    LEFT OUTER JOIN
        tax_class tc 
            ON (
                sp.tax_class_id = tc.tax_class_id
            ) 
    LEFT OUTER JOIN
        buyer b 
            ON (
                sp.buyer_id = b.buyer_id
            ) 
    LEFT OUTER JOIN
        article_category ac 
            ON (
                sp.article_category_id = ac.article_category_id
            ) 
    LEFT OUTER JOIN
        (
            SELECT
                sku_nbr,
                disc_sku_nbr,
                avg_sales,
                copy_sku_nbr,
                copy_sku_pct 
            FROM
                (SELECT
                    zwr.sku_nbr,
                    zww.disc_sku_nbr,
                    zww.avg_sales,
                    zww.copy_sku_nbr,
                    zww.copy_sku_pct,
                    ROW_NUMBER() OVER (PARTITION 
                BY
                    zwr.sku_nbr 
                ORDER BY
                    zwr.created_tstmp DESC,
                    zwr.record_id DESC) rn 
                FROM
                    sap_zth_web_record_pre zwr 
                INNER JOIN
                    sap_zth_web_wip_pre zww 
                        ON zwr.record_id = zww.record_id 
                WHERE
                    zwr.sku_nbr IS NOT NULL) nz_alias 
            WHERE
                rn = 1
            ) znewitem 
                ON sp.sku_nbr = znewitem.sku_nbr 
        LEFT OUTER JOIN
            (
                SELECT
                    ATT.Product_id AS PRODUCT_ID,
                    ATT.SAP_ATT_VALUE_ID AS pricing_role_id,
                    VAL.SAP_ATT_VALUE_DESC AS pricing_role_desc 
                FROM
                    SAP_ATTRIBUTE ATT,
                    SAP_ATT_VALUE VAL 
                WHERE
                    ATT.SAP_ATT_CODE_ID = 'PROL' 
                    AND ATT.delete_flag != 'X' 
                    AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID 
                    AND VAL.SAP_ATT_CODE_ID = 'PROL'
            ) PROL 
                ON sp.product_id = prol.Product_id 
        LEFT OUTER JOIN
            MERCHCAT_ORG mco 
                ON (
                    sp.sap_category_id = mco.sap_category_id
                ) 
        LEFT OUTER JOIN
            artmas_pre artmas 
                ON artmas.article_nbr = sp.sku_nbr 
        LEFT OUTER JOIN
            sap_t6wp1t_pre stp 
                ON artmas.proc_rule = stp.procurement_rule_cd 
        LEFT OUTER JOIN
            sku_flavor skf 
                ON artmas.flavor_cd = skf.flavor_cd 
        LEFT OUTER JOIN
            sku_container skc 
                ON artmas.Container_Reqmt_Cd = skc.container_cd 
        LEFT OUTER JOIN
            (
                SELECT
                    brand_cd,
                    brand_name 
                FROM
                    brand 
                WHERE
                    delete_flag = 0
            ) br 
                ON (
                    sp.brand_cd = br.brand_cd
                ) 
        LEFT OUTER JOIN
            brand_classification bc 
                ON (
                    sp.brand_classification_id = bc.brand_classification_id
                )""")

df_37.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_37")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_RPT


spark.sql("""INSERT INTO SKU_PROFILE_RPT SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
SKU_TYPE AS SKU_TYPE,
PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
STATUS_ID AS STATUS_ID,
STATUS_NAME AS STATUS_NAME,
SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
SKU_DESC AS SKU_DESC,
ALT_DESC AS ALT_DESC,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
GL_CATEGORY_CD AS GL_CATEGORY_CD,
GL_CATEGORY_DESC AS GL_CATEGORY_DESC,
BUS_UNIT_ID AS BUS_UNIT_ID,
BUS_UNIT_DESC AS BUS_UNIT_DESC,
SVP_ID AS SVP_ID,
SVP_DESC AS SVP_DESC,
VP_ID AS VP_ID,
VP_NM AS VP_NM,
VP_DESC AS VP_DESC,
CATEGORY_BUYER_ID AS CATEGORY_BUYER_ID,
CATEGORY_BUYER_NM AS CATEGORY_BUYER_NM,
CA_BUYER_ID AS CA_BUYER_ID,
CA_BUYER_NM AS CA_BUYER_NM,
DIRECTOR_ID AS DIRECTOR_ID,
DIRECTOR_NM AS DIRECTOR_DESC,
CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
CA_DIRECTOR_NM AS CA_DIRECTOR_DESC,
PRICING_ROLE_ID AS PRICING_ROLE_ID,
PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
SAP_CLASS_ID AS SAP_CLASS_ID,
SAP_CLASS_DESC AS SAP_CLASS_DESC,
SAP_DEPT_ID AS SAP_DEPT_ID,
SAP_DEPT_DESC AS SAP_DEPT_DESC,
CONSUM_ID AS CONSUM_ID,
CONSUM_DESC AS CONSUM_DESC,
SEGMENT_ID AS SEGMENT_ID,
SEGMENT_DESC AS SEGMENT_DESC,
SAP_DIVISION_ID AS SAP_DIVISION_ID,
SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
null AS VENDOR_SUBRANGE_CD,
PURCH_GROUP_ID AS PURCH_GROUP_ID,
PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
COUNTRY_CD AS COUNTRY_CD,
COUNTRY_NAME AS COUNTRY_NAME,
IMPORT_FLAG AS IMPORT_FLAG,
IMPORT_DESC AS IMPORT_DESC,
HTS_CODE_ID AS HTS_CODE_ID,
HTS_CODE_DESC AS HTS_CODE_DESC,
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
BUYER_NAME AS BUYER_NAME,
PURCH_COST_AMT AS PURCH_COST_AMT,
NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
TAX_CLASS_ID AS TAX_CLASS_ID,
TAX_CLASS_DESC AS TAX_CLASS_DESC,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
VALUATION_CLASS_DESC AS VALUATION_CLASS_DESC,
BRAND_CD AS BRAND_CD,
BRAND_NAME AS BRAND_NAME,
BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
BRAND_CLASSIFICATION_NAME AS BRAND_CLASSIFICATION_NAME,
OWNBRAND_FLAG AS OWNBRAND_FLAG,
OWNBRAND_DESC AS OWNBRAND_DESC,
STATELINE_FLAG AS STATELINE_FLAG,
SIGN_TYPE_CD AS SIGN_TYPE_CD,
SIGN_TYPE_DESC AS SIGN_TYPE_DESC,
OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
INIT_MKDN_DT AS INIT_MKDN_DT,
DISC_START_DT AS DISC_START_DT,
ADD_DT AS ADD_DT,
DELETE_DT AS DELETE_DT,
FIRST_SALE_DT AS FIRST_SALE_DT,
LAST_SALE_DT AS LAST_SALE_DT,
FIRST_INV_DT AS FIRST_INV_DT,
LAST_INV_DT AS LAST_INV_DT,
BASE_NBR AS BASE_NBR,
BP_COLOR_ID AS BP_COLOR_ID,
BP_SIZE_ID AS BP_SIZE_ID,
BP_BREED_ID AS BP_BREED_ID,
BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
RTV_DEPT_CD AS RTV_DEPT_CD,
RTV_DESC AS RTV_DESC,
HAZ_FLAG AS HAZ_FLAG,
AEROSOL_FLAG AS AEROSOL_FLAG,
GL_ACCT_NBR AS GL_ACCT_NBR,
ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC,
ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
COMPONENT_FLAG AS COMPONENT_FLAG,
ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
ZDISCO_SCHED_TYPE_DESC AS ZDISCO_SCHED_TYPE_DESC,
ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
ZDISCO_MKDN_SCHED_DESC AS ZDISCO_MKDN_SCHED_DESC,
ZDISCO_PID_DT AS ZDISCO_PID_DT,
ZDISCO_START_DT AS ZDISCO_START_DT,
ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
ZDISCO_DC_DT AS ZDISCO_DC_DT,
ZDISCO_STR_DT AS ZDISCO_STR_DT,
ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
SAP_DEMAND_GROUP AS OPT_DEMAND_GROUP,
SAP_PRODUCT_LINE AS OPT_PRODUCT_LINE,
SAP_PRICE_FAMILY AS OPT_PRICE_FAMILY,
EFFECTIVE_SIZE AS OPT_EFFECTIVE_SIZE,
SELL_UNITS AS OPT_SELL_UNITS,
UNIT_PRICE AS OPT_UNIT_PRICE,
PRICE AS OPT_MOD_PRICE,
COST AS OPT_MOD_COST,
IS_NEW AS OPT_IS_NEW,
ERROR_CODE AS OPT_ERROR_CODE,
RELATIONSHIP AS OPT_RELATIONSHIP,
METRIC AS OPT_METRIC,
DIFF AS OPT_DIFF,
ASSOC_PROD AS OPT_ASSOC_PROD,
DELETE_OPTION AS OPT_DELETE_OPTION,
DISC_SKU_NBR AS DISC_SKU_NBR,
AVG_SALES AS AVG_SALES,
COPY_SKU_NBR AS COPY_SKU_NBR,
COPY_SKU_PCT AS COPY_SKU_PCT,
PROCUREMENT_RULE_CD AS PROCUREMENT_RULE_CD,
PROCUREMENT_RULE_DESC AS PROCUREMENT_RULE_DESC,
CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
CATEGORY_REPLENISHMENT_MGR_ID AS CATEGORY_REPLENISHMENT_MGR_ID,
CATEGORY_REPLENISHMENT_MGR_NM AS CATEGORY_REPLENISHMENT_MGR_NM,
CREATED_BY AS CREATED_BY,
SHELF_LIFE_REM_CNT AS SHELF_LIFE_REM_CNT,
TEMP_SKU AS TEMP_SKU,
CONTENTS_METRIC_AMT AS CONTENTS_METRICS,
CONTENTS_UNITS_METRIC_CD AS CONTENTS_UNITS_METRICS,
FLAVOR_CD AS FLAVOR_CD,
FLAVOR_DESC AS FLAVOR_DESC,
MATERIAL AS BASIC_MATERIAL,
PACKAGING_CD AS CONTAINER_CD,
PACKAGING_DEC AS CONTAINER_DESC,
RX_FLAG AS RX_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM SQ_Shortcut_To_SKU_PROFILE_37""")