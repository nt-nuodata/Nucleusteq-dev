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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_price_costs_pre_plsql")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_store_price_costs_pre_plsql", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SOURCE_VENDOR_PRE_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  DELIV_EFF_DT AS DELIV_EFF_DT,
  DELIV_END_DT AS DELIV_END_DT,
  VENDOR_TYPE AS VENDOR_TYPE
FROM
  SOURCE_VENDOR_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SOURCE_VENDOR_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_SITE_PROFILE_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  COUNTRY_CD AS COUNTRY_CD,
  LAST_SALE_DT AS LAST_SALE_DT,
  COND_UNIT AS COND_UNIT,
  HIST_SALE_FLAG AS HIST_SALE_FLAG,
  HIST_INV_FLAG AS HIST_INV_FLAG,
  CURR_ORDER_FLAG AS CURR_ORDER_FLAG,
  CURR_SAP_LISTED_FLAG AS CURR_SAP_LISTED_FLAG,
  CURR_POG_LISTED_FLAG AS CURR_POG_LISTED_FLAG,
  CURR_CATALOG_FLAG AS CURR_CATALOG_FLAG,
  CURR_DOTCOM_FLAG AS CURR_DOTCOM_FLAG,
  PROJ_ORDER_FLAG AS PROJ_ORDER_FLAG,
  AD_PRICE_AMT AS AD_PRICE_AMT,
  REGULAR_PRICE_AMT AS REGULAR_PRICE_AMT,
  NAT_PRICE_AMT AS NAT_PRICE_AMT,
  PETPERKS_PRICE_AMT AS PETPERKS_PRICE_AMT,
  LOC_AD_PRICE_AMT AS LOC_AD_PRICE_AMT,
  LOC_REGULAR_PRICE_AMT AS LOC_REGULAR_PRICE_AMT,
  LOC_NAT_PRICE_AMT AS LOC_NAT_PRICE_AMT,
  LOC_PETPERKS_PRICE_AMT AS LOC_PETPERKS_PRICE_AMT
FROM
  SKU_SITE_PROFILE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_SKU_SITE_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_QUERY_ARGUMENTS1_2


query_2 = f"""SELECT
  JOB_NAME AS JOB_NAME,
  LAST_CHANGE_DT AS LAST_CHANGE_DT,
  LAST_CHANGE_USER_ID AS LAST_CHANGE_USER_ID,
  TABLE_NAME AS TABLE_NAME,
  SQL_DESC AS SQL_DESC,
  SQL_TX AS SQL_TX,
  SQL_TX2 AS SQL_TX2,
  SQL_TX3 AS SQL_TX3
FROM
  QUERY_ARGUMENTS"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_QUERY_ARGUMENTS1_2")

# COMMAND ----------
# DBTITLE 1, ASQ_LOAD_SKU_STORE_PRICE_COSTS_PRE_3


query_3 = f"""SELECT
  's_sku_store_price_cost_pre_plsql' AS MAP_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("ASQ_LOAD_SKU_STORE_PRICE_COSTS_PRE_3")

# COMMAND ----------
# DBTITLE 1, OUT_MPLT_GENERIC_SQL_16


query_4 = f"""SELECT
  JOB_NAME AS MAP_NAME
FROM
  ASQ_LOAD_SKU_STORE_PRICE_COSTS_PRE_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("mGSmGS_Input")

# COMMAND ----------
# DBTITLE 1, INP_MPLT_GENERIC_SQL_5


query_5 = f"""SELECT
  MAP_NAME AS MAP_NAME,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  mGSmGS_Input"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("INP_MPLT_GENERIC_SQL_5")

# COMMAND ----------
# DBTITLE 1, EXP_START_TIME_6


query_6 = f"""SELECT
  MAP_NAME AS MAP_NAME,
  TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') AS var_SESS_START_TIME,
  TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') AS SESS_START_TIME,
  'INSERT INTO QUERY_LOG  VALUES(' || CHR(39) || MAP_NAME || CHR(39) || ',TO_DATE(' || CHR(39) || TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') || CHR(39) || ',' || CHR(39) || 'YYYY-MM-DD HH:MI:SS AM' || CHR(39) || '),NULL, 0,0,' || CHR(39) || 'JOB_STARTED' || CHR(39) || ', 0, 0, 0, 0, ' || CHR(39) || 'Netezza User' || CHR(39) || ')' AS SQL_QUERY_LOG_INSERT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  INP_MPLT_GENERIC_SQL_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_START_TIME_6")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_INSERT_7


query_7 = f"""SELECT
  MAP_NAME AS null,
  SESS_START_TIME AS null,
  SQL_QUERY_LOG_INSERT AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_START_TIME_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("SQL_QUERY_LOG_INSERT_7")

# COMMAND ----------
# DBTITLE 1, EXP_ONE_ROW_FILTER_8


query_8 = f"""SELECT
  MAP_NAME_output AS MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  var_COUNTER + 1 AS var_COUNTER,
  var_COUNTER + 1 + 1 + 1 + 1 + 1 + 1 AS out_COUNTER,
  var_COUNTER AS var_COUNTER_prev,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_LOG_INSERT_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("EXP_ONE_ROW_FILTER_8")

# COMMAND ----------
# DBTITLE 1, FIL_ONE_ROW_9


query_9 = f"""SELECT
  MAP_NAME_output AS MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  out_COUNTER AS out_COUNTER,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_ONE_ROW_FILTER_8
WHERE
  out_COUNTER = 2"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("FIL_ONE_ROW_9")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_ARG_DATA_FETCH_10


query_10 = f"""SELECT
  MAP_NAME_output AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_ONE_ROW_9"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("SQL_QUERY_ARG_DATA_FETCH_10")

# COMMAND ----------
# DBTITLE 1, EXP_TXT_CONCAT_11


query_11 = f"""SELECT
  SQLError AS SQLError,
  IN_MAP_NAME_output AS IN_MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  out_SQL_TX || DECODE(TRUE, ISNULL(out_SQL_TX2), ' ', out_SQL_TX2) || DECODE(TRUE, ISNULL(out_SQL_TX3), ' ', out_SQL_TX3) AS out_SQL_TXT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_ARG_DATA_FETCH_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("EXP_TXT_CONCAT_11")

# COMMAND ----------
# DBTITLE 1, SQL_RUN_SQL_FROM_QUERY_ARG_12


query_12 = f"""SELECT
  out_SQL_TXT AS null,
  SQLError AS null,
  IN_MAP_NAME_output AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_TXT_CONCAT_11"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("SQL_RUN_SQL_FROM_QUERY_ARG_12")

# COMMAND ----------
# DBTITLE 1, EXP_ERROR_MSG_13


query_13 = f"""SELECT
  DECODE(
    TRUE,
    ISNULL(SQLError)
    AND ISNULL(SQL_Error_output),
    'NO ERRORS ENCOUNTERED',
    'error  = ' || SQLError
  ) AS out_SQL_Error,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_RUN_SQL_FROM_QUERY_ARG_12"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("EXP_ERROR_MSG_13")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_UPDATE_14


query_14 = f"""SELECT
  null AS null,
  null AS null,
  out_SQL_Error AS null,
  null AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_ERROR_MSG_13
UNION ALL
SELECT
  MAP_NAME_output AS null,
  NumRowsAffected AS null,
  null AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_RUN_SQL_FROM_QUERY_ARG_12"""

df_14 = spark.sql(query_14)

df_14.createOrReplaceTempView("SQL_QUERY_LOG_UPDATE_14")

# COMMAND ----------
# DBTITLE 1, EXP_OUTPUT_15


query_15 = f"""SELECT
  MAP_NAME_output AS MAP_NAME,
  DECODE(
    TRUE,
    Sql_Error_output = 'NO ERRORS ENCOUNTERED',
    'SUCCEEDED',
    ABORT('FAILURE IN SQL')
  ) AS out_MPLT_STATUS,
  Sql_Error_output AS out_MPLT_SQL_ERROR,
  SESSSTARTTIME AS out_JOB_START_DATE,
  SQLError AS SQLError,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_LOG_UPDATE_14"""

df_15 = spark.sql(query_15)

df_15.createOrReplaceTempView("EXP_OUTPUT_15")

# COMMAND ----------
# DBTITLE 1, OUT_MPLT_GENERIC_SQL


query_16 = f"""SELECT
  MAP_NAME AS MAP_NAME1,
  out_MPLT_STATUS AS MPLT_STATUS,
  out_MPLT_SQL_ERROR AS MPLT_SQL_ERROR,
  out_JOB_START_DATE AS JOB_START_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_OUTPUT_15"""

df_16 = spark.sql(query_16)

df_16.createOrReplaceTempView("OUT_MPLT_GENERIC_SQL_16")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_VENDOR_DAY_18


query_18 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  DELETE_IND AS DELETE_IND,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  DELIV_EFF_DT AS DELIV_EFF_DT,
  DELIV_END_DT AS DELIV_END_DT,
  REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  ROUNDING_PROFILE_CD AS ROUNDING_PROFILE_CD,
  COUNTRY_CD AS COUNTRY_CD,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  LOAD_DT AS LOAD_DT
FROM
  SKU_VENDOR_DAY"""

df_18 = spark.sql(query_18)

df_18.createOrReplaceTempView("Shortcut_To_SKU_VENDOR_DAY_18")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SUPPLY_CHAIN_19


query_19 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  DIRECT_VENDOR_ID AS DIRECT_VENDOR_ID,
  SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  FROM_LOCATION_ID AS FROM_LOCATION_ID
FROM
  SUPPLY_CHAIN"""

df_19 = spark.sql(query_19)

df_19.createOrReplaceTempView("Shortcut_To_SUPPLY_CHAIN_19")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_PROFILE_20


query_20 = f"""SELECT
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

df_20 = spark.sql(query_20)

df_20.createOrReplaceTempView("Shortcut_To_SKU_PROFILE_20")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_STORE_VENDOR_DAY_21


query_21 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
  LOAD_DT AS LOAD_DT
FROM
  SKU_STORE_VENDOR_DAY"""

df_21 = spark.sql(query_21)

df_21.createOrReplaceTempView("Shortcut_To_SKU_STORE_VENDOR_DAY_21")

# COMMAND ----------
# DBTITLE 1, QUERY_LOG_TXT


spark.sql("""INSERT INTO
  QUERY_LOG_TXT
SELECT
  MAP_NAME1 AS JOB_NAME,
  MAP_NAME1 AS JOB_NAME,
  MAP_NAME1 AS JOB_NAME,
  JOB_START_DATE AS QUERY_START_TSTMP,
  JOB_START_DATE AS QUERY_START_TSTMP,
  JOB_START_DATE AS QUERY_START_TSTMP,
  NULL AS QUERY_END_TSTMP,
  NULL AS RESULT_NBR,
  NULL AS ERROR_NBR,
  MPLT_STATUS AS ERROR_TX,
  MPLT_STATUS AS ERROR_TX,
  MPLT_STATUS AS ERROR_TX,
  NULL AS ROWS_AFFECTED,
  NULL AS ROWS_INSERTED,
  NULL AS ROWS_UPDATED,
  NULL AS ROWS_DELETED,
  NULL AS EXEC_USER
FROM
  OUT_MPLT_GENERIC_SQL_16""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_price_costs_pre_plsql")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_store_price_costs_pre_plsql", mainWorkflowId, parentName)
