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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_zth_web_wip_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_zth_web_wip_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTH_WEB_WIP_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  RECORD_ID AS RECORD_ID,
  MATKL AS MATKL,
  WSTAW AS WSTAW,
  POG_REPLACE AS POG_REPLACE,
  DISC_ARTICLE AS DISC_ARTICLE,
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
  DPR_SKU AS DPR_SKU,
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
  CREATED_ON AS CREATED_ON,
  CREATE_TIME AS CREATE_TIME,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_ON AS LAST_CHANGED_ON,
  LAST_CHANGE_TIME AS LAST_CHANGE_TIME,
  DC36 AS DC36,
  S2910 AS S2910,
  S2930 AS S2930,
  AVG_SALES AS AVG_SALES,
  SUBC AS SUBC,
  SPID AS SPID,
  DC38 AS DC38,
  AVG_CATWEB AS AVG_CATWEB,
  INIT_BUY AS INIT_BUY,
  FORC_ARTICLE AS FORC_ARTICLE,
  FORC_PCT AS FORC_PCT,
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
  FORC_SIGN AS FORC_SIGN,
  PRITEM AS PRITEM,
  DC43 AS DC43
FROM
  ZTH_WEB_WIP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTH_WEB_WIP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTH_WEB_WIP_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  RECORD_ID AS RECORD_ID,
  MATKL AS MATKL,
  WSTAW AS WSTAW,
  POG_REPLACE AS POG_REPLACE,
  DISC_ARTICLE AS DISC_ARTICLE,
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
  DPR_SKU AS DPR_SKU,
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
  CREATED_ON AS CREATED_ON,
  CREATE_TIME AS CREATE_TIME,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_ON AS LAST_CHANGED_ON,
  LAST_CHANGE_TIME AS LAST_CHANGE_TIME,
  DC36 AS DC36,
  S2910 AS S2910,
  S2930 AS S2930,
  AVG_SALES AS AVG_SALES,
  SUBC AS SUBC,
  SPID AS SPID,
  DC38 AS DC38,
  AVG_CATWEB AS AVG_CATWEB,
  INIT_BUY AS INIT_BUY,
  FORC_ARTICLE AS FORC_ARTICLE,
  FORC_PCT AS FORC_PCT,
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
  FORC_SIGN AS FORC_SIGN,
  PRITEM AS PRITEM,
  DC43 AS DC43,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTH_WEB_WIP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTH_WEB_WIP_1")

# COMMAND ----------
# DBTITLE 1, FILTRANS_2


query_2 = f"""SELECT
  MANDT AS MANDT,
  RECORD_ID AS RECORD_ID,
  MATKL AS MATKL,
  WSTAW AS WSTAW,
  POG_REPLACE AS POG_REPLACE,
  DISC_ARTICLE AS DISC_ARTICLE,
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
  DPR_SKU AS DPR_SKU,
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
  CREATED_ON AS CREATED_ON,
  CREATE_TIME AS CREATE_TIME,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_ON AS LAST_CHANGED_ON,
  LAST_CHANGE_TIME AS LAST_CHANGE_TIME,
  DC36 AS DC36,
  S2910 AS S2910,
  S2930 AS S2930,
  AVG_SALES AS AVG_SALES,
  SUBC AS SUBC,
  SPID AS SPID,
  DC38 AS DC38,
  AVG_CATWEB AS AVG_CATWEB,
  INIT_BUY AS INIT_BUY,
  FORC_ARTICLE AS FORC_ARTICLE,
  FORC_PCT AS FORC_PCT,
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
  FORC_SIGN AS FORC_SIGN,
  PRITEM AS PRITEM,
  DC43 AS DC43,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ZTH_WEB_WIP_1
WHERE
  MANDT = '100'"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FILTRANS_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


query_3 = f"""SELECT
  IFF(
    Is_Number(DISC_ARTICLE),
    TO_INTEGER(DISC_ARTICLE),
    NULL
  ) AS o_DISC_SKU_NBR,
  IFF(
    ISNULL(DUE_IN_STORE)
    OR LENGTH(DUE_IN_STORE) < 8
    OR DUE_IN_STORE = '00000000',
    NULL,
    TO_DATE(DUE_IN_STORE, 'YYYYMMDD')
  ) AS o_DUE_IN_STORE,
  IFF(Is_Number(DPR_SKU), TO_INTEGER(DPR_SKU), NULL) AS o_DPR_SKU_NBR,
  IFF(
    ISNULL(CREATED_ON)
    OR ISNULL(CREATE_TIME)
    OR LENGTH(CREATED_ON) < 8
    OR LENGTH(CREATE_TIME) < 6
    OR CREATED_ON = '00000000'
    OR CREATE_TIME = '000000',
    NULL,
    CONCAT(CREATED_ON, CREATE_TIME)
  ) AS conact_CREATED_TSTMP,
  IFF(
    ISNULL(
      IFF(
        ISNULL(CREATED_ON)
        OR ISNULL(CREATE_TIME)
        OR LENGTH(CREATED_ON) < 8
        OR LENGTH(CREATE_TIME) < 6
        OR CREATED_ON = '00000000'
        OR CREATE_TIME = '000000',
        NULL,
        CONCAT(CREATED_ON, CREATE_TIME)
      )
    ),
    NULL,
    TO_DATE(
      IFF(
        ISNULL(CREATED_ON)
        OR ISNULL(CREATE_TIME)
        OR LENGTH(CREATED_ON) < 8
        OR LENGTH(CREATE_TIME) < 6
        OR CREATED_ON = '00000000'
        OR CREATE_TIME = '000000',
        NULL,
        CONCAT(CREATED_ON, CREATE_TIME)
      ),
      'YYYYMMDDHH24MISS'
    )
  ) AS o_CREATED_TSTMP,
  IFF(
    ISNULL(LAST_CHANGED_ON)
    OR ISNULL(LAST_CHANGE_TIME)
    OR LENGTH(LAST_CHANGED_ON) < 8
    OR LENGTH(LAST_CHANGE_TIME) < 6
    OR LAST_CHANGED_ON = '00000000'
    OR LAST_CHANGE_TIME = '000000',
    NULL,
    CONCAT(LAST_CHANGED_ON, LAST_CHANGE_TIME)
  ) AS concat_LAST_CHANGED_TSTMP,
  IFF(
    ISNULL(
      IFF(
        ISNULL(LAST_CHANGED_ON)
        OR ISNULL(LAST_CHANGE_TIME)
        OR LENGTH(LAST_CHANGED_ON) < 8
        OR LENGTH(LAST_CHANGE_TIME) < 6
        OR LAST_CHANGED_ON = '00000000'
        OR LAST_CHANGE_TIME = '000000',
        NULL,
        CONCAT(LAST_CHANGED_ON, LAST_CHANGE_TIME)
      )
    ),
    NULL,
    TO_DATE(
      IFF(
        ISNULL(LAST_CHANGED_ON)
        OR ISNULL(LAST_CHANGE_TIME)
        OR LENGTH(LAST_CHANGED_ON) < 8
        OR LENGTH(LAST_CHANGE_TIME) < 6
        OR LAST_CHANGED_ON = '00000000'
        OR LAST_CHANGE_TIME = '000000',
        NULL,
        CONCAT(LAST_CHANGED_ON, LAST_CHANGE_TIME)
      ),
      'YYYYMMDDHH24MISS'
    )
  ) AS o_LAST_CHANGED_TSTMP,
  IFF(
    Is_Number(FORC_ARTICLE),
    TO_INTEGER(FORC_ARTICLE),
    NULL
  ) AS o_COPY_SKU_NBR,
  IFF(
    Is_Number(FORC_ARTICLE),
    TO_INTEGER(FORC_ARTICLE),
    0
  ) AS var_FORC_ARTICLE,
  IFF(
    Is_Number(FORC_PCT),
    TO_INTEGER(FORC_PCT) * IFF(FORC_SIGN = '-', -1, 1),
    NULL
  ) AS o_COPY_SKU_PCT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FILTRANS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, SAP_ZTH_WEB_WIP_PRE


spark.sql("""INSERT INTO
  SAP_ZTH_WEB_WIP_PRE
SELECT
  F2.RECORD_ID AS RECORD_ID,
  F2.MATKL AS MATKL,
  F2.WSTAW AS WSTAW,
  F2.POG_REPLACE AS POG_REPLACE,
  E3.o_DISC_SKU_NBR AS DISC_SKU_NBR,
  F2.RTV_DISP AS RTV_DISP,
  F2.SELL_THROUGH AS SELL_THROUGH,
  F2.PRES_MGR AS PRES_MGR,
  F2.NOTIFY_PRICE AS NOTIFY_PRICE,
  F2.PRICE_MGR AS PRICE_MGR,
  F2.KHOP AS KHOP,
  F2.COLOR AS COLOR,
  F2.SIZE1 AS SIZE1,
  F2.FLAVOR AS FLAVOR,
  F2.RTV AS RTV,
  F2.DISCIPLINE AS DISCIPLINE,
  F2.STATUS_CODE AS STATUS_CODE,
  F2.PLAN_GROUP AS PLAN_GROUP,
  F2.STATELINETACK AS STATELINETACK,
  E3.o_DUE_IN_STORE AS DUE_IN_STORE,
  F2.QTY_FOR_DC4 AS QTY_FOR_DC4,
  F2.RDPRF AS RDPRF,
  F2.APU_QTY AS APU_QTY,
  F2.USITEM AS USITEM,
  F2.CAITEM AS CAITEM,
  F2.US_CA_DIR_ITEM AS US_CA_DIR_ITEM,
  F2.INLINE AS INLINE,
  E3.o_DPR_SKU_NBR AS DPR_SKU_NBR,
  F2.DPR_PLUS AS DPR_PLUS,
  F2.DPR_PERC AS DPR_PERC,
  F2.DPR_MIN_QTY AS DPR_MIN_QTY,
  F2.OTB AS OTB,
  F2.BUYER_SUGGEST AS BUYER_SUGGEST,
  F2.BUYER_A AS BUYER_A,
  F2.BUYER_B AS BUYER_B,
  F2.BUYER_C AS BUYER_C,
  F2.BUYER_D AS BUYER_D,
  F2.BUYER_E AS BUYER_E,
  F2.BUYER_F AS BUYER_F,
  F2.DC8 AS DC8,
  F2.DC9 AS DC9,
  F2.DC10 AS DC10,
  F2.DC12 AS DC12,
  F2.S2920 AS S2920,
  F2.ALLOC AS ALLOC,
  F2.PRODUCT_CODE AS PRODUCT_CODE,
  F2.BASE_NUMBER AS BASE_NUMBER,
  F2.BASE_DESC AS BASE_DESC,
  F2.SLT_CAT_CODE AS SLT_CAT_CODE,
  F2.SUB_PROD_CODE AS SUB_PROD_CODE,
  F2.SADDLE AS SADDLE,
  F2.SIZE_CHART_NUM AS SIZE_CHART_NUM,
  F2.CASE_PACK_QTY AS CASE_PACK_QTY,
  F2.HAZ_MAT AS HAZ_MAT,
  F2.AEROSOL AS AEROSOL,
  F2.ROLL AS ROLL,
  F2.OVER_SIZE AS OVER_SIZE,
  F2.PSM_LABEL AS PSM_LABEL,
  F2.OWNS AS OWNS,
  F2.BASE_GEN AS BASE_GEN,
  F2.DCLR AS DCLR,
  F2.DSZE AS DSZE,
  F2.DBRD AS DBRD,
  F2.DFRT AS DFRT,
  F2.STOR AS STOR,
  F2.WEB AS WEB,
  F2.CATL AS CATL,
  F2.DTCD AS DTCD,
  F2.DLBL AS DLBL,
  F2.PROJECT_ID AS PROJECT_ID,
  F2.CREATED_BY AS CREATED_BY,
  E3.o_CREATED_TSTMP AS CREATED_TSTMP,
  F2.LAST_CHANGED_BY AS LAST_CHANGED_BY,
  E3.o_LAST_CHANGED_TSTMP AS LAST_CHANGED_TSTMP,
  F2.DC36 AS DC36,
  F2.S2910 AS S2910,
  F2.S2930 AS S2930,
  F2.AVG_SALES AS AVG_SALES,
  F2.SUBC AS SUBC,
  F2.SPID AS SPID,
  F2.DC38 AS DC38,
  F2.AVG_CATWEB AS AVG_CATWEB,
  F2.INIT_BUY AS INIT_BUY,
  E3.o_COPY_SKU_NBR AS COPY_SKU_NBR,
  E3.o_COPY_SKU_PCT AS COPY_SKU_PCT,
  F2.ALL_STORES AS ALL_STORES,
  F2.LIM_STORES AS LIM_STORES,
  F2.WEB_STYLE AS WEB_STYLE,
  F2.WEB_STYLE_TEXT AS WEB_STYLE_TEXT,
  F2.WEB_HAZD AS WEB_HAZD,
  F2.DC41 AS DC41,
  F2.DC14 AS DC14,
  F2.DC16 AS DC16,
  F2.DC18 AS DC18,
  F2.EXP_DAYS AS EXP_DAYS,
  F2.MHDRZ AS MHDRZ,
  F2.DC39 AS DC39,
  F2.DC40 AS DC40,
  F2.DC42 AS DC42,
  F2.FORC_SIGN AS COPY_SKU_PCT_SIGN,
  F2.PRITEM AS PRITEM,
  F2.DC43 AS DC43
FROM
  FILTRANS_2 F2
  INNER JOIN EXPTRANS_3 E3 ON F2.Monotonically_Increasing_Id = E3.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_zth_web_wip_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_zth_web_wip_pre", mainWorkflowId, parentName)
