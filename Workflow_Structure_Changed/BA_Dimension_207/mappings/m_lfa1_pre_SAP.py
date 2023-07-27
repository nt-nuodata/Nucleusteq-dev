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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lfa1_pre_SAP")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_lfa1_pre_SAP", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LFA1_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  LIFNR AS LIFNR,
  LAND1 AS LAND1,
  NAME1 AS NAME1,
  NAME2 AS NAME2,
  NAME3 AS NAME3,
  NAME4 AS NAME4,
  ORT01 AS ORT01,
  ORT02 AS ORT02,
  PFACH AS PFACH,
  PSTL2 AS PSTL2,
  PSTLZ AS PSTLZ,
  REGIO AS REGIO,
  SORTL AS SORTL,
  STRAS AS STRAS,
  ADRNR AS ADRNR,
  MCOD1 AS MCOD1,
  MCOD2 AS MCOD2,
  MCOD3 AS MCOD3,
  ANRED AS ANRED,
  BAHNS AS BAHNS,
  BBBNR AS BBBNR,
  BBSNR AS BBSNR,
  BEGRU AS BEGRU,
  BRSCH AS BRSCH,
  BUBKZ AS BUBKZ,
  DATLT AS DATLT,
  DTAMS AS DTAMS,
  DTAWS AS DTAWS,
  ERDAT AS ERDAT,
  ERNAM AS ERNAM,
  ESRNR AS ESRNR,
  KONZS AS KONZS,
  KTOKK AS KTOKK,
  KUNNR AS KUNNR,
  LNRZA AS LNRZA,
  LOEVM AS LOEVM,
  SPERR AS SPERR,
  SPERM AS SPERM,
  SPRAS AS SPRAS,
  STCD1 AS STCD1,
  STCD2 AS STCD2,
  STKZA AS STKZA,
  STKZU AS STKZU,
  TELBX AS TELBX,
  TELF1 AS TELF1,
  TELF2 AS TELF2,
  TELFX AS TELFX,
  TELTX AS TELTX,
  TELX1 AS TELX1,
  XCPDK AS XCPDK,
  XZEMP AS XZEMP,
  VBUND AS VBUND,
  FISKN AS FISKN,
  STCEG AS STCEG,
  STKZN AS STKZN,
  SPERQ AS SPERQ,
  GBORT AS GBORT,
  GBDAT AS GBDAT,
  SEXKZ AS SEXKZ,
  KRAUS AS KRAUS,
  REVDB AS REVDB,
  QSSYS AS QSSYS,
  KTOCK AS KTOCK,
  PFORT AS PFORT,
  WERKS AS WERKS,
  LTSNA AS LTSNA,
  WERKR AS WERKR,
  PLKAL AS PLKAL,
  DUEFL AS DUEFL,
  TXJCD AS TXJCD,
  SPERZ AS SPERZ,
  SCACD AS SCACD,
  SFRGR AS SFRGR,
  LZONE AS LZONE,
  XLFZA AS XLFZA,
  DLGRP AS DLGRP,
  FITYP AS FITYP,
  STCDT AS STCDT,
  REGSS AS REGSS,
  ACTSS AS ACTSS,
  STCD3 AS STCD3,
  STCD4 AS STCD4,
  IPISP AS IPISP,
  TAXBS AS TAXBS,
  PROFS AS PROFS,
  STGDL AS STGDL,
  EMNFR AS EMNFR,
  LFURL AS LFURL,
  J_1KFREPRE AS J_1KFREPRE,
  J_1KFTBUS AS J_1KFTBUS,
  J_1KFTIND AS J_1KFTIND,
  CONFS AS CONFS,
  UPDAT AS UPDAT,
  UPTIM AS UPTIM,
  NODEL AS NODEL,
  QSSYSDAT AS QSSYSDAT,
  PODKZB AS PODKZB,
  FISKU AS FISKU,
  STENR AS STENR
FROM
  LFA1"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_LFA1_0")

# COMMAND ----------
# DBTITLE 1, SQ_LFA1_1


query_1 = f"""SELECT
  LIFNR AS LIFNR,
  LAND1 AS LAND1,
  NAME1 AS NAME1,
  ORT01 AS ORT01,
  PSTLZ AS PSTLZ,
  REGIO AS REGIO,
  STRAS AS STRAS,
  BRSCH AS BRSCH,
  KTOKK AS KTOKK,
  TELF1 AS TELF1,
  TELFX AS TELFX,
  WERKS AS WERKS,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  sappr3.Shortcut_to_LFA1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_LFA1_1")

# COMMAND ----------
# DBTITLE 1, LFA1_PRE


spark.sql("""INSERT INTO
  LFA1_PRE
SELECT
  LIFNR AS LIFNR,
  LAND1 AS LAND1,
  NAME1 AS NAME1,
  ORT01 AS ORT01,
  PSTLZ AS PSTLZ,
  REGIO AS REGIO,
  STRAS AS STRAS,
  BRSCH AS BRSCH,
  KTOKK AS KTOKK,
  TELF1 AS TELF1,
  TELFX AS TELFX,
  WERKS AS WERKS
FROM
  SQ_LFA1_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lfa1_pre_SAP")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_lfa1_pre_SAP", mainWorkflowId, parentName)
