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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_wk_DELETE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sales_ranking_wk_DELETE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SALES_RANKING_DATE_PRE_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  RANKING_WEEK_DT AS RANKING_WEEK_DT
FROM
  SALES_RANKING_DATE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SALES_RANKING_DATE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_RANKING_DATE_PRE_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  RANKING_WEEK_DT AS RANKING_WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SALES_RANKING_DATE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SALES_RANKING_DATE_PRE_1")

# COMMAND ----------
# DBTITLE 1, AGG_PASS_PROCESSING_WK_2


query_2 = f"""SELECT
  RANKING_WEEK_DT AS RANKING_WEEK_DT,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SALES_RANKING_DATE_PRE_1
GROUP BY
  RANKING_WEEK_DT"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("AGG_PASS_PROCESSING_WK_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SALES_RANKING_WK_3


query_3 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  COMP_CURR_FLAG AS COMP_CURR_FLAG,
  SALES_CURR_FLAG AS SALES_CURR_FLAG,
  TOTAL_52WK_SALES_AMT AS TOTAL_52WK_SALES_AMT,
  TOTAL_52WK_COMP_STORES_AMT AS TOTAL_52WK_COMP_STORES_AMT,
  STORE_TOTAL_SALES_PCT AS STORE_TOTAL_SALES_PCT,
  RUNNING_SUM_TOTAL_SALES_PCT AS RUNNING_SUM_TOTAL_SALES_PCT,
  TOTAL_SALES_RANKING_CD AS TOTAL_SALES_RANKING_CD,
  TOTAL_SALES_RANKING_LEVEL AS TOTAL_SALES_RANKING_LEVEL,
  MERCH_52WK_SALES_AMT AS MERCH_52WK_SALES_AMT,
  MERCH_52WK_COMP_STORES_AMT AS MERCH_52WK_COMP_STORES_AMT,
  STORE_MERCH_SALES_PCT AS STORE_MERCH_SALES_PCT,
  RUNNING_SUM_MERCH_SALES_PCT AS RUNNING_SUM_MERCH_SALES_PCT,
  MERCH_SALES_RANKING_CD AS MERCH_SALES_RANKING_CD,
  MERCH_SALES_RANKING_LEVEL AS MERCH_SALES_RANKING_LEVEL,
  SERVICES_52WK_SALES_AMT AS SERVICES_52WK_SALES_AMT,
  SERVICES_52WK_COMP_STORES_AMT AS SERVICES_52WK_COMP_STORES_AMT,
  STORE_SERVICES_SALES_PCT AS STORE_SERVICES_SALES_PCT,
  RUNNING_SUM_SERVICES_SALES_PCT AS RUNNING_SUM_SERVICES_SALES_PCT,
  SERVICES_SALES_RANKING_CD AS SERVICES_SALES_RANKING_CD,
  SERVICES_SALES_RANKING_LEVEL AS SERVICES_SALES_RANKING_LEVEL,
  SALON_52WK_SALES_AMT AS SALON_52WK_SALES_AMT,
  SALON_52WK_COMP_STORES_AMT AS SALON_52WK_COMP_STORES_AMT,
  STORE_SALON_SALES_PCT AS STORE_SALON_SALES_PCT,
  RUNNING_SUM_SALON_SALES_PCT AS RUNNING_SUM_SALON_SALES_PCT,
  SALON_SALES_RANKING_CD AS SALON_SALES_RANKING_CD,
  SALON_SALES_RANKING_LEVEL AS SALON_SALES_RANKING_LEVEL,
  TRAINING_52WK_SALES_AMT AS TRAINING_52WK_SALES_AMT,
  TRAINING_52WK_COMP_STORES_AMT AS TRAINING_52WK_COMP_STORES_AMT,
  STORE_TRAINING_SALES_PCT AS STORE_TRAINING_SALES_PCT,
  RUNNING_SUM_TRAINING_SALES_PCT AS RUNNING_SUM_TRAINING_SALES_PCT,
  TRAINING_SALES_RANKING_CD AS TRAINING_SALES_RANKING_CD,
  TRAINING_SALES_RANKING_LEVEL AS TRAINING_SALES_RANKING_LEVEL,
  HOTEL_DDC_52WK_SALES_AMT AS HOTEL_DDC_52WK_SALES_AMT,
  HOTEL_DDC_52WK_COMP_STORES_AMT AS HOTEL_DDC_52WK_COMP_STORES_AMT,
  STORE_HOTEL_DDC_SALES_PCT AS STORE_HOTEL_DDC_SALES_PCT,
  RUNNING_SUM_HOTEL_DDC_SALES_PCT AS RUNNING_SUM_HOTEL_DDC_SALES_PCT,
  HOTEL_DDC_SALES_RANKING_CD AS HOTEL_DDC_SALES_RANKING_CD,
  HOTEL_DDC_SALES_RANKING_LEVEL AS HOTEL_DDC_SALES_RANKING_LEVEL,
  CONSUMABLES_52WK_SALES_AMT AS CONSUMABLES_52WK_SALES_AMT,
  CONSUMABLES_52WK_COMP_STORES_AMT AS CONSUMABLES_52WK_COMP_STORES_AMT,
  STORE_CONSUMABLES_SALES_PCT AS STORE_CONSUMABLES_SALES_PCT,
  RUNNING_SUM_CONSUMABLES_SALES_PCT AS RUNNING_SUM_CONSUMABLES_SALES_PCT,
  CONSUMABLES_SALES_RANKING_CD AS CONSUMABLES_SALES_RANKING_CD,
  CONSUMABLES_SALES_RANKING_LEVEL AS CONSUMABLES_SALES_RANKING_LEVEL,
  HARDGOODS_52WK_SALES_AMT AS HARDGOODS_52WK_SALES_AMT,
  HARDGOODS_52WK_COMP_STORES_AMT AS HARDGOODS_52WK_COMP_STORES_AMT,
  STORE_HARDGOODS_SALES_PCT AS STORE_HARDGOODS_SALES_PCT,
  RUNNING_SUM_HARDGOODS_SALES_PCT AS RUNNING_SUM_HARDGOODS_SALES_PCT,
  HARDGOODS_SALES_RANKING_CD AS HARDGOODS_SALES_RANKING_CD,
  HARDGOODS_SALES_RANKING_LEVEL AS HARDGOODS_SALES_RANKING_LEVEL,
  SPECIALTY_52WK_SALES_AMT AS SPECIALTY_52WK_SALES_AMT,
  SPECIALTY_52WK_COMP_STORES_AMT AS SPECIALTY_52WK_COMP_STORES_AMT,
  STORE_SPECIALTY_SALES_PCT AS STORE_SPECIALTY_SALES_PCT,
  RUNNING_SUM_SPECIALTY_SALES_PCT AS RUNNING_SUM_SPECIALTY_SALES_PCT,
  SPECIALTY_SALES_RANKING_CD AS SPECIALTY_SALES_RANKING_CD,
  SPECIALTY_SALES_RANKING_LEVEL AS SPECIALTY_SALES_RANKING_LEVEL,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SALES_RANKING_WK"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_SALES_RANKING_WK_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_RANKING_WK_4


query_4 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  COMP_CURR_FLAG AS COMP_CURR_FLAG,
  SALES_CURR_FLAG AS SALES_CURR_FLAG,
  TOTAL_52WK_SALES_AMT AS TOTAL_52WK_SALES_AMT,
  TOTAL_52WK_COMP_STORES_AMT AS TOTAL_52WK_COMP_STORES_AMT,
  STORE_TOTAL_SALES_PCT AS STORE_TOTAL_SALES_PCT,
  RUNNING_SUM_TOTAL_SALES_PCT AS RUNNING_SUM_TOTAL_SALES_PCT,
  TOTAL_SALES_RANKING_CD AS TOTAL_SALES_RANKING_CD,
  TOTAL_SALES_RANKING_LEVEL AS TOTAL_SALES_RANKING_LEVEL,
  MERCH_52WK_SALES_AMT AS MERCH_52WK_SALES_AMT,
  MERCH_52WK_COMP_STORES_AMT AS MERCH_52WK_COMP_STORES_AMT,
  STORE_MERCH_SALES_PCT AS STORE_MERCH_SALES_PCT,
  RUNNING_SUM_MERCH_SALES_PCT AS RUNNING_SUM_MERCH_SALES_PCT,
  MERCH_SALES_RANKING_CD AS MERCH_SALES_RANKING_CD,
  MERCH_SALES_RANKING_LEVEL AS MERCH_SALES_RANKING_LEVEL,
  SERVICES_52WK_SALES_AMT AS SERVICES_52WK_SALES_AMT,
  SERVICES_52WK_COMP_STORES_AMT AS SERVICES_52WK_COMP_STORES_AMT,
  STORE_SERVICES_SALES_PCT AS STORE_SERVICES_SALES_PCT,
  RUNNING_SUM_SERVICES_SALES_PCT AS RUNNING_SUM_SERVICES_SALES_PCT,
  SERVICES_SALES_RANKING_CD AS SERVICES_SALES_RANKING_CD,
  SERVICES_SALES_RANKING_LEVEL AS SERVICES_SALES_RANKING_LEVEL,
  SALON_52WK_SALES_AMT AS SALON_52WK_SALES_AMT,
  SALON_52WK_COMP_STORES_AMT AS SALON_52WK_COMP_STORES_AMT,
  STORE_SALON_SALES_PCT AS STORE_SALON_SALES_PCT,
  RUNNING_SUM_SALON_SALES_PCT AS RUNNING_SUM_SALON_SALES_PCT,
  SALON_SALES_RANKING_CD AS SALON_SALES_RANKING_CD,
  SALON_SALES_RANKING_LEVEL AS SALON_SALES_RANKING_LEVEL,
  TRAINING_52WK_SALES_AMT AS TRAINING_52WK_SALES_AMT,
  TRAINING_52WK_COMP_STORES_AMT AS TRAINING_52WK_COMP_STORES_AMT,
  STORE_TRAINING_SALES_PCT AS STORE_TRAINING_SALES_PCT,
  RUNNING_SUM_TRAINING_SALES_PCT AS RUNNING_SUM_TRAINING_SALES_PCT,
  TRAINING_SALES_RANKING_CD AS TRAINING_SALES_RANKING_CD,
  TRAINING_SALES_RANKING_LEVEL AS TRAINING_SALES_RANKING_LEVEL,
  HOTEL_DDC_52WK_SALES_AMT AS HOTEL_DDC_52WK_SALES_AMT,
  HOTEL_DDC_52WK_COMP_STORES_AMT AS HOTEL_DDC_52WK_COMP_STORES_AMT,
  STORE_HOTEL_DDC_SALES_PCT AS STORE_HOTEL_DDC_SALES_PCT,
  RUNNING_SUM_HOTEL_DDC_SALES_PCT AS RUNNING_SUM_HOTEL_DDC_SALES_PCT,
  HOTEL_DDC_SALES_RANKING_CD AS HOTEL_DDC_SALES_RANKING_CD,
  HOTEL_DDC_SALES_RANKING_LEVEL AS HOTEL_DDC_SALES_RANKING_LEVEL,
  CONSUMABLES_52WK_SALES_AMT AS CONSUMABLES_52WK_SALES_AMT,
  CONSUMABLES_52WK_COMP_STORES_AMT AS CONSUMABLES_52WK_COMP_STORES_AMT,
  STORE_CONSUMABLES_SALES_PCT AS STORE_CONSUMABLES_SALES_PCT,
  RUNNING_SUM_CONSUMABLES_SALES_PCT AS RUNNING_SUM_CONSUMABLES_SALES_PCT,
  CONSUMABLES_SALES_RANKING_CD AS CONSUMABLES_SALES_RANKING_CD,
  CONSUMABLES_SALES_RANKING_LEVEL AS CONSUMABLES_SALES_RANKING_LEVEL,
  HARDGOODS_52WK_SALES_AMT AS HARDGOODS_52WK_SALES_AMT,
  HARDGOODS_52WK_COMP_STORES_AMT AS HARDGOODS_52WK_COMP_STORES_AMT,
  STORE_HARDGOODS_SALES_PCT AS STORE_HARDGOODS_SALES_PCT,
  RUNNING_SUM_HARDGOODS_SALES_PCT AS RUNNING_SUM_HARDGOODS_SALES_PCT,
  HARDGOODS_SALES_RANKING_CD AS HARDGOODS_SALES_RANKING_CD,
  HARDGOODS_SALES_RANKING_LEVEL AS HARDGOODS_SALES_RANKING_LEVEL,
  SPECIALTY_52WK_SALES_AMT AS SPECIALTY_52WK_SALES_AMT,
  SPECIALTY_52WK_COMP_STORES_AMT AS SPECIALTY_52WK_COMP_STORES_AMT,
  STORE_SPECIALTY_SALES_PCT AS STORE_SPECIALTY_SALES_PCT,
  RUNNING_SUM_SPECIALTY_SALES_PCT AS RUNNING_SUM_SPECIALTY_SALES_PCT,
  SPECIALTY_SALES_RANKING_CD AS SPECIALTY_SALES_RANKING_CD,
  SPECIALTY_SALES_RANKING_LEVEL AS SPECIALTY_SALES_RANKING_LEVEL,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SALES_RANKING_WK_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_SALES_RANKING_WK_4")

# COMMAND ----------
# DBTITLE 1, FIL_RECENT_TXNS_5


query_5 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SALES_RANKING_WK_4
WHERE
  WEEK_DT > ADD_TO_DATE(now(), 'DD', - 45)"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_RECENT_TXNS_5")

# COMMAND ----------
# DBTITLE 1, JNR_SALES_RANKING_WK_DEL_6


query_6 = f"""SELECT
  DETAIL.RANKING_WEEK_DT AS RANKING_WEEK_DT,
  MASTER.WEEK_DT AS WEEK_DT,
  MASTER.LOCATION_ID AS LOCATION_ID,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_RECENT_TXNS_5 MASTER
  INNER JOIN AGG_PASS_PROCESSING_WK_2 DETAIL ON MASTER.WEEK_DT = DETAIL.RANKING_WEEK_DT"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("JNR_SALES_RANKING_WK_DEL_6")

# COMMAND ----------
# DBTITLE 1, UPD_SALES_RANING_WK_DEL_7


query_7 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_SALES_RANKING_WK_DEL_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_SALES_RANING_WK_DEL_7")

# COMMAND ----------
# DBTITLE 1, SALES_RANKING_WK


spark.sql("""MERGE INTO SALES_RANKING_WK AS TARGET
USING
  UPD_SALES_RANING_WK_DEL_7 AS SOURCE ON TARGET.WEEK_DT = SOURCE.WEEK_DT
  AND TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  WHEN MATCHED THEN DELETE""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_wk_DELETE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sales_ranking_wk_DELETE", mainWorkflowId, parentName)
