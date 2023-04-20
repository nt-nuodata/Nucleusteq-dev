# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_EVENT_PRE_0


df_0=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
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
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_PRE""")

df_0.createOrReplaceTempView("MA_EVENT_PRE_0")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_1


df_1=spark.sql("""
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

df_1.createOrReplaceTempView("MA_EVENT_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_EVENT_2


df_2=spark.sql("""--Get Inserts
SELECT ma_event_id, product_id, country_cd, start_dt, end_dt, ma_event_type_id,
       ma_event_source_id, location_id, ma_event_desc, em_vendor_funding_id,
       em_comment, em_bill_alt_vendor_flag, em_alt_vendor_id,
       em_alt_vendor_name, em_alt_vendor_country_cd,
       em_vendor_id, em_vendor_name, em_vendor_country_cd, ma_amt,
       ma_max_amt, CURRENT_DATE update_dt, CURRENT_DATE load_dt,
       'I' ins_upd_del_flag
  FROM ma_event_pre
 WHERE ma_event_source_id = 1
   AND ma_event_id NOT IN (SELECT ma_event_id
                             FROM ma_event)
UNION
--Get Updates
SELECT pre.*, CURRENT_DATE update_dt, e.load_dt,
       CASE WHEN pre.product_id <> e.product_id OR
                 pre.country_cd <> e.country_cd OR
                 pre.start_dt <> e.start_dt OR
                 pre.end_dt <> e.end_dt OR
                 pre.location_id <> e.location_id OR
                 pre.ma_event_type_id <> e.ma_event_type_id OR
                 pre.ma_amt <> e.ma_amt
            THEN 'U'
            ELSE 'C'
        END ins_upd_del_flag
  FROM (SELECT ma_event_id, product_id, country_cd, start_dt, end_dt, ma_event_type_id,
               ma_event_source_id, location_id, ma_event_desc,
               em_vendor_funding_id, em_comment, em_bill_alt_vendor_flag,
               em_alt_vendor_id, em_alt_vendor_name, em_alt_vendor_country_cd,
               em_vendor_id, em_vendor_name, em_vendor_country_cd, ma_amt, ma_max_amt
          FROM ma_event_pre
        MINUS
        SELECT ma_event_id, product_id, country_cd, start_dt, end_dt, ma_event_type_id,
               ma_event_source_id, location_id, ma_event_desc,
               em_vendor_funding_id, em_comment, em_bill_alt_vendor_flag,
               em_alt_vendor_id, em_alt_vendor_name, em_alt_vendor_country_cd,
               em_vendor_id, em_vendor_name, em_vendor_country_cd, ma_amt, ma_max_amt
          FROM ma_event) pre,
       ma_event e
 WHERE pre.ma_event_id = e.ma_event_id
UNION
--Get Update Record for MA_EVENT_RESTATE_HIST Table
SELECT e.ma_event_id, e.product_id, e.country_cd, e.start_dt, e.end_dt, e.ma_event_type_id,
       e.ma_event_source_id, e.location_id, e.ma_event_desc, e.em_vendor_funding_id,
       e.em_comment, e.em_bill_alt_vendor_flag, e.em_alt_vendor_id,
       e.em_alt_vendor_name, e.em_alt_vendor_country_cd,
       e.em_vendor_id, e.em_vendor_name, e.em_vendor_country_cd, e.ma_amt,
       e.ma_max_amt, CURRENT_DATE update_dt, e.load_dt,
       'R' ins_upd_del_flag
  FROM (SELECT ma_event_id, product_id, country_cd, start_dt, end_dt, ma_event_type_id,
               ma_event_source_id, location_id, ma_event_desc,
               em_vendor_funding_id, em_comment, em_bill_alt_vendor_flag,
               em_alt_vendor_id, em_alt_vendor_name, em_alt_vendor_country_cd,
               em_vendor_id, em_vendor_name, em_vendor_country_cd, ma_amt, ma_max_amt
          FROM ma_event_pre
        MINUS
        SELECT ma_event_id, product_id, country_cd, start_dt, end_dt, ma_event_type_id,
               ma_event_source_id, location_id, ma_event_desc,
               em_vendor_funding_id, em_comment, em_bill_alt_vendor_flag,
               em_alt_vendor_id, em_alt_vendor_name, em_alt_vendor_country_cd,
               em_vendor_id, em_vendor_name, em_vendor_country_cd, ma_amt, ma_max_amt
          FROM ma_event) pre,
       ma_event e
 WHERE pre.ma_event_id = e.ma_event_id
   AND (pre.product_id <> e.product_id OR
        pre.country_cd <> e.country_cd OR
        pre.start_dt <> e.start_dt OR
        pre.end_dt <> e.end_dt OR
        pre.location_id <> e.location_id OR
        pre.ma_event_type_id <> e.ma_event_type_id OR
        pre.ma_amt <> e.ma_amt)
UNION
--Get Deletes
SELECT ma_event_id, product_id, country_cd, start_dt, end_dt,
       ma_event_type_id, ma_event_source_id, location_id, ma_event_desc,
       em_vendor_funding_id, em_comment, em_bill_alt_vendor_flag, em_alt_vendor_id,
       em_alt_vendor_name, em_alt_vendor_country_cd, em_vendor_id, em_vendor_name,
       em_vendor_country_cd, ma_amt, ma_max_amt, CURRENT_DATE update_dt, load_dt,
       'D' ins_upd_del_flag
  FROM ma_event
 WHERE ma_event_source_id = 1
   AND start_dt >= (SELECT week_dt + 1 FROM days WHERE day_dt = CURRENT_DATE - 96)
   AND ma_event_id NOT IN (SELECT ma_event_id
                             FROM ma_event_pre)""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_MA_EVENT_2")

# COMMAND ----------
# DBTITLE 1, FTR_MA_EVENT_3


df_3=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
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
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_EVENT_2 
    WHERE
        INS_UPD_DEL_FLAG <> 'R'""")

df_3.createOrReplaceTempView("FTR_MA_EVENT_3")

# COMMAND ----------
# DBTITLE 1, UPD_MA_EVENT_4


df_4=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
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
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FTR_MA_EVENT_3""")

df_4.createOrReplaceTempView("UPD_MA_EVENT_4")

# COMMAND ----------
# DBTITLE 1, FTR_MA_EVENT_RESTATE_HIST_5


df_5=spark.sql("""
    SELECT
        LOAD_DT AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
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
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MA_EVENT_2 
    WHERE
        INS_UPD_DEL_FLAG = 'I' 
        OR INS_UPD_DEL_FLAG = 'R' 
        OR INS_UPD_DEL_FLAG = 'D'""")

df_5.createOrReplaceTempView("FTR_MA_EVENT_RESTATE_HIST_5")

# COMMAND ----------
# DBTITLE 1, EXP_MA_EVENT_RESTATE_HIST_6


df_6=spark.sql("""
    SELECT
        LOAD_DT AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
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
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        IFF(INS_UPD_DEL_FLAG = 'R',
        'U',
        INS_UPD_DEL_FLAG) AS INS_UPD_DEL_FLAG_out,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FTR_MA_EVENT_RESTATE_HIST_5""")

df_6.createOrReplaceTempView("EXP_MA_EVENT_RESTATE_HIST_6")

# COMMAND ----------
# DBTITLE 1, MA_EVENT


spark.sql("""INSERT INTO MA_EVENT SELECT MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
OFFER_ID AS OFFER_ID,
null AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
null AS MOVEMENT_ID,
null AS VALUATION_CLASS_CD,
null AS GL_ACCT_NBR,
null AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
null AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
null AS SOURCE_VENDOR_ID,
null AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
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
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
UPDATE_DT AS UPDATE_DT,
UPDATE_DT AS UPDATE_DT,
UPDATE_DT AS UPDATE_DT,
UPDATE_DT AS UPDATE_DT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT FROM UPD_MA_EVENT_4""")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_RESTATE_HIST


spark.sql("""INSERT INTO MA_EVENT_RESTATE_HIST SELECT LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
LOAD_DT AS LOAD_DT,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_ID AS MA_EVENT_ID,
null AS OFFER_ID,
null AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
START_DT AS START_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
null AS MOVEMENT_ID,
null AS VALUATION_CLASS_CD,
null AS GL_ACCT_NBR,
null AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO AS FISCAL_MO,
null AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
null AS SOURCE_VENDOR_ID,
null AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_DESC AS MA_EVENT_DESC,
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
null AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_PCT_IND AS MA_PCT_IND,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
INS_UPD_DEL_FLAG_out AS INS_UPD_DEL_FLAG,
INS_UPD_DEL_FLAG_out AS INS_UPD_DEL_FLAG,
INS_UPD_DEL_FLAG_out AS INS_UPD_DEL_FLAG,
INS_UPD_DEL_FLAG_out AS INS_UPD_DEL_FLAG,
INS_UPD_DEL_FLAG_out AS INS_UPD_DEL_FLAG FROM EXP_MA_EVENT_RESTATE_HIST_6""")