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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_stpo_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_shipper_stpo_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STPO_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  STLTY AS STLTY,
  STLNR AS STLNR,
  STLKN AS STLKN,
  STPOZ AS STPOZ,
  DATUV AS DATUV,
  TECHV AS TECHV,
  AENNR AS AENNR,
  LKENZ AS LKENZ,
  VGKNT AS VGKNT,
  VGPZL AS VGPZL,
  ANDAT AS ANDAT,
  ANNAM AS ANNAM,
  AEDAT AS AEDAT,
  AENAM AS AENAM,
  IDNRK AS IDNRK,
  PSWRK AS PSWRK,
  POSTP AS POSTP,
  POSNR AS POSNR,
  SORTF AS SORTF,
  MEINS AS MEINS,
  MENGE AS MENGE,
  FMENG AS FMENG,
  AUSCH AS AUSCH,
  AVOAU AS AVOAU,
  NETAU AS NETAU,
  SCHGT AS SCHGT,
  BEIKZ AS BEIKZ,
  ERSKZ AS ERSKZ,
  RVREL AS RVREL,
  SANFE AS SANFE,
  SANIN AS SANIN,
  SANKA AS SANKA,
  SANKO AS SANKO,
  SANVS AS SANVS,
  STKKZ AS STKKZ,
  REKRI AS REKRI,
  REKRS AS REKRS,
  CADPO AS CADPO,
  NFMAT AS NFMAT,
  NLFZT AS NLFZT,
  VERTI AS VERTI,
  ALPOS AS ALPOS,
  EWAHR AS EWAHR,
  EKGRP AS EKGRP,
  LIFZT AS LIFZT,
  LIFNR AS LIFNR,
  PREIS AS PREIS,
  PEINH AS PEINH,
  WAERS AS WAERS,
  SAKTO AS SAKTO,
  ROANZ AS ROANZ,
  ROMS1 AS ROMS1,
  ROMS2 AS ROMS2,
  ROMS3 AS ROMS3,
  ROMEI AS ROMEI,
  ROMEN AS ROMEN,
  RFORM AS RFORM,
  UPSKZ AS UPSKZ,
  VALKZ AS VALKZ,
  LTXSP AS LTXSP,
  POTX1 AS POTX1,
  POTX2 AS POTX2,
  OBJTY AS OBJTY,
  MATKL AS MATKL,
  WEBAZ AS WEBAZ,
  DOKAR AS DOKAR,
  DOKNR AS DOKNR,
  DOKVR AS DOKVR,
  DOKTL AS DOKTL,
  CSSTR AS CSSTR,
  CLASS AS CLASS,
  KLART AS KLART,
  POTPR AS POTPR,
  AWAKZ AS AWAKZ,
  INSKZ AS INSKZ,
  VCEKZ AS VCEKZ,
  VSTKZ AS VSTKZ,
  VACKZ AS VACKZ,
  EKORG AS EKORG,
  CLOBK AS CLOBK,
  CLMUL AS CLMUL,
  CLALT AS CLALT,
  CVIEW AS CVIEW,
  KNOBJ AS KNOBJ,
  LGORT AS LGORT,
  KZKUP AS KZKUP,
  INTRM AS INTRM,
  TPEKZ AS TPEKZ,
  STVKN AS STVKN,
  DVDAT AS DVDAT,
  DVNAM AS DVNAM,
  DSPST AS DSPST,
  ALPST AS ALPST,
  ALPRF AS ALPRF,
  ALPGR AS ALPGR,
  KZNFP AS KZNFP,
  NFGRP AS NFGRP,
  NFEAG AS NFEAG,
  KNDVB AS KNDVB,
  KNDBZ AS KNDBZ,
  KSTTY AS KSTTY,
  KSTNR AS KSTNR,
  KSTKN AS KSTKN,
  KSTPZ AS KSTPZ,
  CLSZU AS CLSZU,
  KZCLB AS KZCLB,
  AEHLP AS AEHLP,
  PRVBE AS PRVBE,
  NLFZV AS NLFZV,
  NLFMV AS NLFMV,
  IDPOS AS IDPOS,
  IDHIS AS IDHIS,
  IDVAR AS IDVAR,
  ALEKZ AS ALEKZ,
  ITMID AS ITMID,
  GUID AS GUID,
  ITSOB AS ITSOB,
  RFPNT AS RFPNT,
  GUIDX AS GUIDX
FROM
  STPO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STPO_0")

# COMMAND ----------
# DBTITLE 1, SQ_STPO_1


query_1 = f"""SELECT
  Shortcut_to_STPO_0.STLTY,
  Shortcut_to_STPO_0.STLNR,
  Shortcut_to_STPO_0.STLKN,
  Shortcut_to_STPO_0.STPOZ,
  Shortcut_to_STPO_0.DATUV,
  Shortcut_to_STPO_0.ANDAT,
  Shortcut_to_STPO_0.ANNAM,
  Shortcut_to_STPO_0.AEDAT,
  Shortcut_to_STPO_0.AENAM,
  Shortcut_to_STPO_0.IDNRK,
  Shortcut_to_STPO_0.POSNR,
  Shortcut_to_STPO_0.MEINS,
  Shortcut_to_STPO_0.MENGE,
  Shortcut_to_STPO_0.POTX1,
  Shortcut_to_STPO_0.MATKL,
  Shortcut_to_STPO_0.STVKN,
  Shortcut_to_STPO_0.ALEKZ,
  Shortcut_to_STPO_0.GUIDX
FROM
  SAPPR3.Shortcut_to_STPO_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_STPO_1")

# COMMAND ----------
# DBTITLE 1, SHIPPER_STPO_PRE


spark.sql("""INSERT INTO
  SHIPPER_STPO_PRE
SELECT
  STLTY AS BOM_CATEGORY_ID,
  STLNR AS BILL_OF_MATERIAL,
  STLKN AS ITEM_NODE,
  STPOZ AS INTERNAL_CNTR,
  DATUV AS VALID_FROM_DT,
  ANDAT AS CREATED_ON_DT,
  ANNAM AS CREATED_BY_NAME,
  AEDAT AS CHANGED_ON_DT,
  AENAM AS CHANGED_BY_NAME,
  IDNRK AS BOM_COMPONENT,
  POSNR AS ITEM_NBR,
  MEINS AS COMPONENT_UNIT,
  MENGE AS COMPONENT_QTY,
  POTX1 AS LINE_1_ITEM_TEXT,
  MATKL AS MERCH_CATEGORY_ID,
  STVKN AS INHERITED_NODE_NBR,
  ALEKZ AS ALE_IND,
  GUIDX AS ITEM_CHG_STATUS_ID
FROM
  SQ_STPO_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_stpo_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_shipper_stpo_pre", mainWorkflowId, parentName)
