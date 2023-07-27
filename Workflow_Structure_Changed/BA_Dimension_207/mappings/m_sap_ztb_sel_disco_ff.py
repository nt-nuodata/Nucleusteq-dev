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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_ztb_sel_disco_ff")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_ztb_sel_disco_ff", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_SEL_DISCO_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  WERKS AS WERKS,
  OSTATUS AS OSTATUS,
  STYPE AS STYPE,
  MATKL AS MATKL,
  PID AS PID,
  ZZSTATCD AS ZZSTATCD,
  ZZDSCS AS ZZDSCS,
  ZZMD_SCH_ID AS ZZMD_SCH_ID,
  ZZMKDN AS ZZMKDN,
  DC_EMAIL AS DC_EMAIL,
  DC_WRN_EMAIL AS DC_WRN_EMAIL,
  DC_OEMAIL AS DC_OEMAIL,
  DC_DISCO_DT AS DC_DISCO_DT,
  ST_DISCO_DT AS ST_DISCO_DT,
  ST_DISCO_OW_DT AS ST_DISCO_OW_DT,
  ST_WOFF_DT AS ST_WOFF_DT,
  VKORG AS VKORG,
  ZMERCH_STOR AS ZMERCH_STOR,
  ZMERCH_WEB AS ZMERCH_WEB,
  ZMERCH_CAT AS ZMERCH_CAT,
  CR_DATE AS CR_DATE,
  CHG_DATE AS CHG_DATE,
  ZZBUYR AS ZZBUYR,
  DESCR AS DESCR,
  OLD_ZZSTATCD AS OLD_ZZSTATCD,
  OLD_ZMERCH_STOR AS OLD_ZMERCH_STOR,
  OLD_ZMERCH_WEB AS OLD_ZMERCH_WEB,
  OLD_ZMERCH_CAT AS OLD_ZMERCH_CAT,
  EKGRP AS EKGRP,
  DSCD AS DSCD,
  ZZMD_OVRD_IND AS ZZMD_OVRD_IND,
  OLD_ZZDSCS AS OLD_ZZDSCS,
  ZTIMES AS ZTIMES
FROM
  ZTB_SEL_DISCO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_SEL_DISCO_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_SEL_DISCO_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  WERKS AS WERKS,
  OSTATUS AS OSTATUS,
  STYPE AS STYPE,
  MATKL AS MATKL,
  PID AS PID,
  ZZSTATCD AS ZZSTATCD,
  ZZDSCS AS ZZDSCS,
  ZZMD_SCH_ID AS ZZMD_SCH_ID,
  ZZMKDN AS ZZMKDN,
  DC_EMAIL AS DC_EMAIL,
  DC_WRN_EMAIL AS DC_WRN_EMAIL,
  DC_OEMAIL AS DC_OEMAIL,
  DC_DISCO_DT AS DC_DISCO_DT,
  ST_DISCO_DT AS ST_DISCO_DT,
  ST_DISCO_OW_DT AS ST_DISCO_OW_DT,
  ST_WOFF_DT AS ST_WOFF_DT,
  VKORG AS VKORG,
  ZMERCH_STOR AS ZMERCH_STOR,
  ZMERCH_WEB AS ZMERCH_WEB,
  ZMERCH_CAT AS ZMERCH_CAT,
  CR_DATE AS CR_DATE,
  CHG_DATE AS CHG_DATE,
  ZZBUYR AS ZZBUYR,
  DESCR AS DESCR,
  OLD_ZZSTATCD AS OLD_ZZSTATCD,
  OLD_ZMERCH_STOR AS OLD_ZMERCH_STOR,
  OLD_ZMERCH_WEB AS OLD_ZMERCH_WEB,
  OLD_ZMERCH_CAT AS OLD_ZMERCH_CAT,
  EKGRP AS EKGRP,
  DSCD AS DSCD,
  ZZMD_OVRD_IND AS ZZMD_OVRD_IND,
  OLD_ZZDSCS AS OLD_ZZDSCS,
  ZTIMES AS ZTIMES,
  EPRGR AS EPRGR,
  DC_ON_ORD AS DC_ON_ORD,
  SORG_IND AS SORG_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTB_SEL_DISCO_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_SEL_DISCO_1")

# COMMAND ----------
# DBTITLE 1, FF_ZTB_SEL_DISCO


spark.sql("""INSERT INTO
  FF_ZTB_SEL_DISCO
SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  WERKS AS WERKS,
  OSTATUS AS OSTATUS,
  STYPE AS STYPE,
  MATKL AS MATKL,
  PID AS PID,
  ZZSTATCD AS ZZSTATCD,
  ZZDSCS AS ZZDSCS,
  ZZMD_SCH_ID AS ZZMD_SCH_ID,
  ZZMKDN AS ZZMKDN,
  DC_EMAIL AS DC_EMAIL,
  DC_WRN_EMAIL AS DC_WRN_EMAIL,
  DC_OEMAIL AS DC_OEMAIL,
  DC_DISCO_DT AS DC_DISCO_DT,
  ST_DISCO_DT AS ST_DISCO_DT,
  ST_DISCO_OW_DT AS ST_DISCO_OW_DT,
  ST_WOFF_DT AS ST_WOFF_DT,
  VKORG AS VKORG,
  ZMERCH_STOR AS ZMERCH_STOR,
  ZMERCH_WEB AS ZMERCH_WEB,
  ZMERCH_CAT AS ZMERCH_CAT,
  CR_DATE AS CR_DATE,
  CHG_DATE AS CHG_DATE,
  ZZBUYR AS ZZBUYR,
  DESCR AS DESCR,
  OLD_ZZSTATCD AS OLD_ZZSTATCD,
  OLD_ZMERCH_STOR AS OLD_ZMERCH_STOR,
  OLD_ZMERCH_WEB AS OLD_ZMERCH_WEB,
  OLD_ZMERCH_CAT AS OLD_ZMERCH_CAT,
  EKGRP AS EKGRP,
  DSCD AS DSCD,
  ZZMD_OVRD_IND AS ZZMD_OVRD_IND,
  OLD_ZZDSCS AS OLD_ZZDSCS,
  ZTIMES AS ZTIMES,
  EPRGR AS EPRGR,
  DC_ON_ORD AS DC_ON_ORD,
  SORG_IND AS SORG_IND
FROM
  SQ_Shortcut_to_ZTB_SEL_DISCO_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_ztb_sel_disco_ff")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_ztb_sel_disco_ff", mainWorkflowId, parentName)
