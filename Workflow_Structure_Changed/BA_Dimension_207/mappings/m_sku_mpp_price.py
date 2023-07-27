# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_mpp_price")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_mpp_price", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_MPP_PRICE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  MPP_TYPE AS MPP_TYPE,
  MPP_PRICE AS MPP_PRICE,
  MPP_INACTIVE AS MPP_INACTIVE,
  MPP_FORM_EFFECT_DT AS MPP_FORM_EFFECT_DT,
  MPP_START_DT AS MPP_START_DT,
  MPP_END_DT AS MPP_END_DT,
  CREATED_DT AS CREATED_DT,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DT AS LAST_CHANGED_DT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_MPP_PRICE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_MPP_PRICE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_MPP_PRICE_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  MPP_TYPE AS MPP_TYPE,
  MPP_PRICE AS MPP_PRICE,
  MPP_INACTIVE AS MPP_INACTIVE,
  MPP_FORM_EFFECT_DT AS MPP_FORM_EFFECT_DT,
  MPP_START_DT AS MPP_START_DT,
  MPP_END_DT AS MPP_END_DT,
  CREATED_DT AS CREATED_DT,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DT AS LAST_CHANGED_DT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_MPP_PRICE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_MPP_PRICE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZMMTT_MPP_VALUES_2


query_2 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  MPP_TYPE AS MPP_TYPE,
  MPP_START AS MPP_START,
  MPP_END AS MPP_END,
  MPP_PRICE AS MPP_PRICE,
  MPP_INACTIVE AS MPP_INACTIVE,
  MPP_CURRENCY AS MPP_CURRENCY,
  MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  MPP_CREATED_BY AS MPP_CREATED_BY,
  MPP_CREATED_ON AS MPP_CREATED_ON,
  MPP_UPDATED_BY AS MPP_UPDATED_BY,
  MPP_UPDATED_ON AS MPP_UPDATED_ON
FROM
  ZMMTT_MPP_VALUES"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_ZMMTT_MPP_VALUES_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZMMTT_MPP_VALUES_3


query_3 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  MPP_TYPE AS MPP_TYPE,
  MPP_START AS MPP_START,
  MPP_END AS MPP_END,
  MPP_PRICE AS MPP_PRICE,
  MPP_INACTIVE AS MPP_INACTIVE,
  MPP_CURRENCY AS MPP_CURRENCY,
  MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  MPP_CREATED_BY AS MPP_CREATED_BY,
  MPP_CREATED_ON AS MPP_CREATED_ON,
  MPP_UPDATED_BY AS MPP_UPDATED_BY,
  MPP_UPDATED_ON AS MPP_UPDATED_ON,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZMMTT_MPP_VALUES_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_ZMMTT_MPP_VALUES_3")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_4


query_4 = f"""SELECT
  LTRIM(RTRIM(MANDT)) AS MANDT_out,
  TO_INTEGER(MATNR) AS MATNR_out,
  LTRIM(RTRIM(MPP_TYPE)) AS MPP_TYPE_out,
  MPP_START AS MPP_START,
  MPP_END AS MPP_END,
  MPP_PRICE AS MPP_PRICE,
  IFF(ltrim(rtrim(MPP_INACTIVE)) = 'X', '1', '0') AS SRC_MPP_INACTIVE,
  LTRIM(RTRIM(MPP_CURRENCY)) AS MPP_CURRENCY_out,
  MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  LTRIM(RTRIM(MPP_CREATED_BY)) AS MPP_CREATED_BY_out,
  MPP_CREATED_ON AS MPP_CREATED_ON,
  LTRIM(RTRIM(MPP_UPDATED_BY)) AS MPP_UPDATED_BY_out,
  MPP_UPDATED_ON AS MPP_UPDATED_ON,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ZMMTT_MPP_VALUES_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXPTRANS_4")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_PROFILE_5


query_5 = f"""SELECT
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
  ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT
FROM
  SKU_PROFILE"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Shortcut_To_SKU_PROFILE_5")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE_6


query_6 = f"""SELECT
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
  Shortcut_To_SKU_PROFILE_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_6")

# COMMAND ----------
# DBTITLE 1, JNR_SKU_PROFILE_7


query_7 = f"""SELECT
  DETAIL.MANDT_out AS MANDT,
  DETAIL.null AS MATNR,
  DETAIL.MATNR_out AS o_MATNR,
  DETAIL.MPP_TYPE_out AS MPP_TYPE,
  DETAIL.MPP_START AS MPP_START,
  DETAIL.MPP_END AS MPP_END,
  DETAIL.MPP_PRICE AS MPP_PRICE,
  DETAIL.SRC_MPP_INACTIVE AS MPP_INACTIVE,
  DETAIL.MPP_CURRENCY_out AS MPP_CURRENCY,
  DETAIL.MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  DETAIL.MPP_CREATED_BY_out AS MPP_CREATED_BY,
  DETAIL.MPP_CREATED_ON AS MPP_CREATED_ON,
  DETAIL.MPP_UPDATED_BY_out AS MPP_UPDATED_BY,
  DETAIL.MPP_UPDATED_ON AS MPP_UPDATED_ON,
  MASTER.PRODUCT_ID AS PRODUCT_ID,
  MASTER.SKU_NBR AS SKU_NBR,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_PROFILE_6 MASTER
  INNER JOIN EXPTRANS_4 DETAIL ON MASTER.SKU_NBR = DETAIL.MATNR_out"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("JNR_SKU_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, JNR_MPP_SKU_PRICE_8


query_8 = f"""SELECT
  DETAIL.PRODUCT_ID AS PRODUCT_ID,
  DETAIL.MPP_TYPE AS MPP_TYPE,
  DETAIL.MPP_PRICE AS MPP_PRICE,
  DETAIL.MPP_INACTIVE AS MPP_INACTIVE,
  DETAIL.MPP_FORM_EFFECT_DT AS MPP_FORM_EFFECT_DT,
  DETAIL.MPP_START_DT AS MPP_START_DT,
  DETAIL.MPP_END_DT AS MPP_END_DT,
  DETAIL.CREATED_DT AS CREATED_DT,
  DETAIL.LAST_CHANGED_BY AS LAST_CHANGED_BY,
  DETAIL.LAST_CHANGED_DT AS LAST_CHANGED_DT,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.MPP_TYPE AS SRC_MPP_TYPE1,
  MASTER.MPP_START AS SRC_MPP_START,
  MASTER.MPP_END AS SRC_MPP_END,
  MASTER.MPP_PRICE AS SRC_MPP_PRICE1,
  MASTER.MPP_INACTIVE AS SRC_MPP_INACTIVE1,
  MASTER.MPP_EFFECT_DATE AS SRC_MPP_EFFECT_DATE,
  MASTER.MPP_CREATED_ON AS SRC_MPP_CREATED_ON,
  MASTER.MPP_UPDATED_BY AS SRC_MPP_UPDATED_BY,
  MASTER.MPP_UPDATED_ON AS SRC_MPP_UPDATED_ON,
  MASTER.PRODUCT_ID AS PRODUCT_ID1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_SKU_PROFILE_7 MASTER
  LEFT JOIN SQ_Shortcut_to_SKU_MPP_PRICE_1 DETAIL ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID
  AND MASTER.MPP_START = DETAIL.MPP_START_DT
  AND MASTER.MPP_TYPE = DETAIL.MPP_TYPE
  AND MASTER.MPP_INACTIVE = DETAIL.MPP_INACTIVE
  AND MASTER.MPP_PRICE = DETAIL.MPP_PRICE
  AND MASTER.MPP_END = DETAIL.MPP_END_DT"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("JNR_MPP_SKU_PRICE_8")

# COMMAND ----------
# DBTITLE 1, EXP_INSERT_UPDATE_LOGIC_9


query_9 = f"""SELECT
  PRODUCT_ID1 AS TGT_PRODUCT_ID,
  PRODUCT_ID1 AS PRODUCT_ID,
  SRC_MPP_TYPE1 AS MPP_TYPE,
  SRC_MPP_START AS MPP_START,
  SRC_MPP_END AS MPP_END,
  SRC_MPP_PRICE1 AS MPP_PRICE,
  SRC_MPP_INACTIVE1 AS SRC_MPP_INACTIVE,
  SRC_MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  SRC_MPP_CREATED_ON AS MPP_CREATED_ON,
  SRC_MPP_UPDATED_BY AS MPP_UPDATED_BY,
  SRC_MPP_UPDATED_ON AS MPP_UPDATED_ON,
  IFF(
    IFF(
      ISNULL(PRODUCT_ID1)
      AND NOT ISNULL(PRODUCT_ID1),
      'INSERT',
      IFF(
        NOT ISNULL(PRODUCT_ID1)
        AND (
          IFF (
            ISNULL(SRC_MPP_EFFECT_DATE),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(SRC_MPP_EFFECT_DATE)
          ) <> IFF (
            ISNULL(MPP_FORM_EFFECT_DT),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(MPP_FORM_EFFECT_DT)
          )
          OR IFF (
            ISNULL(SRC_MPP_CREATED_ON),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(SRC_MPP_CREATED_ON)
          ) <> IFF (
            ISNULL(CREATED_DT),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(CREATED_DT)
          )
          OR IFF(
            ISNULL(SRC_MPP_UPDATED_BY),
            'S',
            SRC_MPP_UPDATED_BY
          ) <> IFF(ISNULL(LAST_CHANGED_BY), 'S', LAST_CHANGED_BY)
          OR IFF (
            ISNULL(SRC_MPP_UPDATED_ON),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(SRC_MPP_UPDATED_ON)
          ) <> IFF (
            ISNULL(LAST_CHANGED_DT),
            TO_DATE('01/01/1900', 'MM/DD/YYYY'),
            TRUNC(LAST_CHANGED_DT)
          )
        ),
        'UPDATE',
        'NOCHANGE'
      )
    ) = 'INSERT',
    sysdate,
    LOAD_TSTMP
  ) AS LOAD_TSTMP,
  sysdate AS UPDATE_TSTMP,
  SRC_MPP_TYPE1 AS TGT_MPP_TYPE,
  MPP_PRICE AS TGT_MPP_PRICE,
  MPP_INACTIVE AS TGT_MPP_INACTIVE,
  MPP_FORM_EFFECT_DT AS TGT_MPP_EFFECT_DT,
  MPP_START_DT AS TGT_MPP_START_DT,
  MPP_END_DT AS TGT_MPP_END_DT,
  CREATED_DT AS TGT_CREATED_DT,
  LAST_CHANGED_BY AS TGT_LAST_CHANGED_BY,
  LAST_CHANGED_DT AS TGT_LAST_CHANGED_DT,
  NULL AS TGT_DELETE_FLAG,
  LOAD_TSTMP AS TGT_LOAD_TSTMP,
  IFF(
    ISNULL(TGT_PRODUCT_ID)
    AND NOT ISNULL(PRODUCT_ID),
    'INSERT',
    IFF(
      NOT ISNULL(TGT_PRODUCT_ID)
      AND (
        IFF (
          ISNULL(MPP_EFFECT_DATE),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(MPP_EFFECT_DATE)
        ) <> IFF (
          ISNULL(TGT_MPP_EFFECT_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(TGT_MPP_EFFECT_DT)
        )
        OR IFF (
          ISNULL(MPP_CREATED_ON),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(MPP_CREATED_ON)
        ) <> IFF (
          ISNULL(TGT_CREATED_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(TGT_CREATED_DT)
        )
        OR IFF(ISNULL(MPP_UPDATED_BY), 'S', MPP_UPDATED_BY) <> IFF(
          ISNULL(TGT_LAST_CHANGED_BY),
          'S',
          TGT_LAST_CHANGED_BY
        )
        OR IFF (
          ISNULL(MPP_UPDATED_ON),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(MPP_UPDATED_ON)
        ) <> IFF (
          ISNULL(TGT_LAST_CHANGED_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(TGT_LAST_CHANGED_DT)
        )
      ),
      'UPDATE',
      'NOCHANGE'
    )
  ) AS Insert_else_update_v,
  IFF(
    ISNULL(PRODUCT_ID1)
    AND NOT ISNULL(PRODUCT_ID1),
    'INSERT',
    IFF(
      NOT ISNULL(PRODUCT_ID1)
      AND (
        IFF (
          ISNULL(SRC_MPP_EFFECT_DATE),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(SRC_MPP_EFFECT_DATE)
        ) <> IFF (
          ISNULL(MPP_FORM_EFFECT_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(MPP_FORM_EFFECT_DT)
        )
        OR IFF (
          ISNULL(SRC_MPP_CREATED_ON),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(SRC_MPP_CREATED_ON)
        ) <> IFF (
          ISNULL(CREATED_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(CREATED_DT)
        )
        OR IFF(
          ISNULL(SRC_MPP_UPDATED_BY),
          'S',
          SRC_MPP_UPDATED_BY
        ) <> IFF(ISNULL(LAST_CHANGED_BY), 'S', LAST_CHANGED_BY)
        OR IFF (
          ISNULL(SRC_MPP_UPDATED_ON),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(SRC_MPP_UPDATED_ON)
        ) <> IFF (
          ISNULL(LAST_CHANGED_DT),
          TO_DATE('01/01/1900', 'MM/DD/YYYY'),
          TRUNC(LAST_CHANGED_DT)
        )
      ),
      'UPDATE',
      'NOCHANGE'
    )
  ) AS Insert_else_update_o,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_MPP_SKU_PRICE_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("EXP_INSERT_UPDATE_LOGIC_9")

# COMMAND ----------
# DBTITLE 1, FILTRANS_10


query_10 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  MPP_TYPE AS MPP_TYPE,
  MPP_START AS MPP_START,
  MPP_END AS MPP_END,
  MPP_PRICE AS MPP_PRICE,
  SRC_MPP_INACTIVE AS SRC_MPP_INACTIVE,
  MPP_EFFECT_DATE AS MPP_EFFECT_DATE,
  MPP_CREATED_ON AS MPP_CREATED_ON,
  MPP_UPDATED_BY AS MPP_UPDATED_BY,
  MPP_UPDATED_ON AS MPP_UPDATED_ON,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  Insert_else_update_o AS Insert_else_update_o,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_INSERT_UPDATE_LOGIC_9
WHERE
  Insert_else_update_o <> 'NOCHANGE'"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("FILTRANS_10")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_11


query_11 = f"""SELECT
  PRODUCT_ID AS Product_id1,
  MPP_TYPE AS MPP_TYPE1,
  MPP_START AS MPP_START1,
  MPP_END AS MPP_END1,
  MPP_PRICE AS MPP_PRICE1,
  MPP_EFFECT_DATE AS MPP_EFFECT_DATE1,
  SRC_MPP_INACTIVE AS SRC_MPP_INACTIVE11,
  MPP_CREATED_ON AS MPP_CREATED_ON1,
  MPP_UPDATED_BY AS MPP_UPDATED_BY1,
  MPP_UPDATED_ON AS MPP_UPDATED_ON1,
  LOAD_TSTMP AS LOAD_TSTMP1,
  UPDATE_TSTMP AS UPDATE_TSTMP1,
  Insert_else_update_o AS Insert_else_update_o,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    Insert_else_update_o = 'INSERT',
    'DD_INSERT',
    IFF(Insert_else_update_o = 'UPDATE', 'DD_UPDATE')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  FILTRANS_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("UPD_INSERT_11")

# COMMAND ----------
# DBTITLE 1, SKU_MPP_PRICE


spark.sql("""MERGE INTO SKU_MPP_PRICE AS TARGET
USING
  UPD_INSERT_11 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.Product_id1
  AND TARGET.MPP_INACTIVE = SOURCE.SRC_MPP_INACTIVE11
  AND TARGET.MPP_START_DT = SOURCE.MPP_START1
  AND TARGET.MPP_PRICE = SOURCE.MPP_PRICE1
  AND TARGET.MPP_TYPE = SOURCE.MPP_TYPE1
  AND TARGET.MPP_END_DT = SOURCE.MPP_END1
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.Product_id1,
  TARGET.MPP_TYPE = SOURCE.MPP_TYPE1,
  TARGET.MPP_PRICE = SOURCE.MPP_PRICE1,
  TARGET.MPP_INACTIVE = SOURCE.SRC_MPP_INACTIVE11,
  TARGET.MPP_FORM_EFFECT_DT = SOURCE.MPP_EFFECT_DATE1,
  TARGET.MPP_START_DT = SOURCE.MPP_START1,
  TARGET.MPP_END_DT = SOURCE.MPP_END1,
  TARGET.CREATED_DT = SOURCE.MPP_CREATED_ON1,
  TARGET.LAST_CHANGED_BY = SOURCE.MPP_UPDATED_BY1,
  TARGET.LAST_CHANGED_DT = SOURCE.MPP_UPDATED_ON1,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP1,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP1
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.MPP_FORM_EFFECT_DT = SOURCE.MPP_EFFECT_DATE1
  AND TARGET.CREATED_DT = SOURCE.MPP_CREATED_ON1
  AND TARGET.LAST_CHANGED_BY = SOURCE.MPP_UPDATED_BY1
  AND TARGET.LAST_CHANGED_DT = SOURCE.MPP_UPDATED_ON1
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP1
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP1 THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.MPP_TYPE,
    TARGET.MPP_PRICE,
    TARGET.MPP_INACTIVE,
    TARGET.MPP_FORM_EFFECT_DT,
    TARGET.MPP_START_DT,
    TARGET.MPP_END_DT,
    TARGET.CREATED_DT,
    TARGET.LAST_CHANGED_BY,
    TARGET.LAST_CHANGED_DT,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.Product_id1,
    SOURCE.MPP_TYPE1,
    SOURCE.MPP_PRICE1,
    SOURCE.SRC_MPP_INACTIVE11,
    SOURCE.MPP_EFFECT_DATE1,
    SOURCE.MPP_START1,
    SOURCE.MPP_END1,
    SOURCE.MPP_CREATED_ON1,
    SOURCE.MPP_UPDATED_BY1,
    SOURCE.MPP_UPDATED_ON1,
    SOURCE.UPDATE_TSTMP1,
    SOURCE.LOAD_TSTMP1
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_mpp_price")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_mpp_price", mainWorkflowId, parentName)