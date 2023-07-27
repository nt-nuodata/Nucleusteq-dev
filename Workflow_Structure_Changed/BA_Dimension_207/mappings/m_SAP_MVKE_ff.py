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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SAP_MVKE_ff")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SAP_MVKE_ff", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MVKE_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  VKORG AS VKORG,
  VTWEG AS VTWEG,
  LVORM AS LVORM,
  VERSG AS VERSG,
  BONUS AS BONUS,
  PROVG AS PROVG,
  SKTOF AS SKTOF,
  VMSTA AS VMSTA,
  VMSTD AS VMSTD,
  AUMNG AS AUMNG,
  LFMNG AS LFMNG,
  EFMNG AS EFMNG,
  SCMNG AS SCMNG,
  SCHME AS SCHME,
  VRKME AS VRKME,
  MTPOS AS MTPOS,
  DWERK AS DWERK,
  PRODH AS PRODH,
  PMATN AS PMATN,
  KONDM AS KONDM,
  KTGRM AS KTGRM,
  MVGR1 AS MVGR1,
  MVGR2 AS MVGR2,
  MVGR3 AS MVGR3,
  MVGR4 AS MVGR4,
  MVGR5 AS MVGR5,
  SSTUF AS SSTUF,
  PFLKS AS PFLKS,
  LSTFL AS LSTFL,
  LSTVZ AS LSTVZ,
  LSTAK AS LSTAK,
  LDVFL AS LDVFL,
  LDBFL AS LDBFL,
  LDVZL AS LDVZL,
  LDBZL AS LDBZL,
  VDVFL AS VDVFL,
  VDBFL AS VDBFL,
  VDVZL AS VDVZL,
  VDBZL AS VDBZL,
  PRAT1 AS PRAT1,
  PRAT2 AS PRAT2,
  PRAT3 AS PRAT3,
  PRAT4 AS PRAT4,
  PRAT5 AS PRAT5,
  PRAT6 AS PRAT6,
  PRAT7 AS PRAT7,
  PRAT8 AS PRAT8,
  PRAT9 AS PRAT9,
  PRATA AS PRATA,
  RDPRF AS RDPRF,
  MEGRU AS MEGRU,
  LFMAX AS LFMAX,
  RJART AS RJART,
  PBIND AS PBIND,
  VAVME AS VAVME,
  MATKC AS MATKC,
  PVMSO AS PVMSO
FROM
  MVKE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_MVKE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MVKE_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  VKORG AS VKORG,
  VTWEG AS VTWEG,
  LVORM AS LVORM,
  VERSG AS VERSG,
  BONUS AS BONUS,
  PROVG AS PROVG,
  SKTOF AS SKTOF,
  VMSTA AS VMSTA,
  VMSTD AS VMSTD,
  AUMNG AS AUMNG,
  LFMNG AS LFMNG,
  EFMNG AS EFMNG,
  SCMNG AS SCMNG,
  SCHME AS SCHME,
  VRKME AS VRKME,
  MTPOS AS MTPOS,
  DWERK AS DWERK,
  PRODH AS PRODH,
  PMATN AS PMATN,
  KONDM AS KONDM,
  KTGRM AS KTGRM,
  MVGR1 AS MVGR1,
  MVGR2 AS MVGR2,
  MVGR3 AS MVGR3,
  MVGR4 AS MVGR4,
  MVGR5 AS MVGR5,
  SSTUF AS SSTUF,
  PFLKS AS PFLKS,
  LSTFL AS LSTFL,
  LSTVZ AS LSTVZ,
  LSTAK AS LSTAK,
  LDVFL AS LDVFL,
  LDBFL AS LDBFL,
  LDVZL AS LDVZL,
  LDBZL AS LDBZL,
  VDVFL AS VDVFL,
  VDBFL AS VDBFL,
  VDVZL AS VDVZL,
  VDBZL AS VDBZL,
  PRAT1 AS PRAT1,
  PRAT2 AS PRAT2,
  PRAT3 AS PRAT3,
  PRAT4 AS PRAT4,
  PRAT5 AS PRAT5,
  PRAT6 AS PRAT6,
  PRAT7 AS PRAT7,
  PRAT8 AS PRAT8,
  PRAT9 AS PRAT9,
  PRATA AS PRATA,
  RDPRF AS RDPRF,
  MEGRU AS MEGRU,
  LFMAX AS LFMAX,
  RJART AS RJART,
  PBIND AS PBIND,
  VAVME AS VAVME,
  MATKC AS MATKC,
  PVMSO AS PVMSO,
  / BEV1 / EMLGRP AS _BEV1_EMLGRP,
  / BEV1 / EMDRCKSPL AS _BEV1_EMDRCKSPL,
  / BEV1 / RPBEZME AS _BEV1_RPBEZME,
  / BEV1 / RPSNS AS _BEV1_RPSNS,
  / BEV1 / RPSFA AS _BEV1_RPSFA,
  / BEV1 / RPSKI AS _BEV1_RPSKI,
  / BEV1 / RPSCO AS _BEV1_RPSCO,
  / BEV1 / RPSSO AS _BEV1_RPSSO,
  PLGTP AS PLGTP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_MVKE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_MVKE_1")

# COMMAND ----------
# DBTITLE 1, MVKE_ff


spark.sql("""INSERT INTO
  MVKE_ff
SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  VKORG AS VKORG,
  VTWEG AS VTWEG,
  LVORM AS LVORM,
  VERSG AS VERSG,
  BONUS AS BONUS,
  PROVG AS PROVG,
  SKTOF AS SKTOF,
  VMSTA AS VMSTA,
  VMSTD AS VMSTD,
  AUMNG AS AUMNG,
  LFMNG AS LFMNG,
  EFMNG AS EFMNG,
  SCMNG AS SCMNG,
  SCHME AS SCHME,
  VRKME AS VRKME,
  MTPOS AS MTPOS,
  DWERK AS DWERK,
  PRODH AS PRODH,
  PMATN AS PMATN,
  KONDM AS KONDM,
  KTGRM AS KTGRM,
  MVGR1 AS MVGR1,
  MVGR2 AS MVGR2,
  MVGR3 AS MVGR3,
  MVGR4 AS MVGR4,
  MVGR5 AS MVGR5,
  SSTUF AS SSTUF,
  PFLKS AS PFLKS,
  LSTFL AS LSTFL,
  LSTVZ AS LSTVZ,
  LSTAK AS LSTAK,
  LDVFL AS LDVFL,
  LDBFL AS LDBFL,
  LDVZL AS LDVZL,
  LDBZL AS LDBZL,
  VDVFL AS VDVFL,
  VDBFL AS VDBFL,
  VDVZL AS VDVZL,
  VDBZL AS VDBZL,
  PRAT1 AS PRAT1,
  PRAT2 AS PRAT2,
  PRAT3 AS PRAT3,
  PRAT4 AS PRAT4,
  PRAT5 AS PRAT5,
  PRAT6 AS PRAT6,
  PRAT7 AS PRAT7,
  PRAT8 AS PRAT8,
  PRAT9 AS PRAT9,
  PRATA AS PRATA,
  RDPRF AS RDPRF,
  MEGRU AS MEGRU,
  LFMAX AS LFMAX,
  RJART AS RJART,
  PBIND AS PBIND,
  VAVME AS VAVME,
  MATKC AS MATKC,
  PVMSO AS PVMSO,
  _BEV1_EMLGRP AS / BEV1 / EMLGRP,
  _BEV1_EMDRCKSPL AS / BEV1 / EMDRCKSPL,
  _BEV1_RPBEZME AS / BEV1 / RPBEZME,
  _BEV1_RPSNS AS / BEV1 / RPSNS,
  _BEV1_RPSFA AS / BEV1 / RPSFA,
  _BEV1_RPSKI AS / BEV1 / RPSKI,
  _BEV1_RPSCO AS / BEV1 / RPSCO,
  _BEV1_RPSSO AS / BEV1 / RPSSO,
  PLGTP AS PLGTP
FROM
  SQ_Shortcut_to_MVKE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SAP_MVKE_ff")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SAP_MVKE_ff", mainWorkflowId, parentName)
