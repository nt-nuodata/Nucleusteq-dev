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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_MerchCat_Org")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_MerchCat_Org", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MERCHCAT_ORG_0


query_0 = f"""SELECT
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
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  MERCHCAT_ORG"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_MERCHCAT_ORG_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MERCHCAT_ORG_1


query_1 = f"""SELECT
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD,
  MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC,
  CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
  CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
  CATEGORY_REPLENISHMENT_MGR_ID AS REPLENISMENT_MGR_ID,
  CATEGORY_REPLENISHMENT_MGR_NM AS REPLENISMENT_MGR_NM,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_MERCHCAT_ORG_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_MERCHCAT_ORG_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchCategory_2


query_2 = f"""SELECT
  MerchCategoryCd AS MerchCategoryCd,
  MerchCategoryDesc AS MerchCategoryDesc,
  MerchClassCd AS MerchClassCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  GLCategoryCd AS GLCategoryCd,
  CategoryAnalystId AS CategoryAnalystId,
  PlanningInd AS PlanningInd
FROM
  MerchCategory"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_MerchCategory_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MerchCategory_3


query_3 = f"""SELECT
  MerchCategoryCd AS MerchCategoryCd,
  MerchCategoryDesc AS MerchCategoryDesc,
  MerchClassCd AS MerchClassCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  GLCategoryCd AS GLCategoryCd,
  CategoryAnalystId AS CategoryAnalystId,
  PlanningInd AS PlanningInd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_MerchCategory_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_MerchCategory_3")

# COMMAND ----------
# DBTITLE 1, LKP_MerchClass_4


query_4 = f"""SELECT
  M.MerchClassCd AS MerchClassCd,
  M.MerchClassDesc AS MerchClassDesc,
  M.MerchDeptCd AS MerchDeptCd,
  M.UpdateTstmp AS UpdateTstmp,
  SStM3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_MerchCategory_3 SStM3
  LEFT JOIN MerchClass M ON M.MerchClassCd = SStM3.M.MerchClassCd"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_MerchClass_4")

# COMMAND ----------
# DBTITLE 1, LKP_MerchDept_5


query_5 = f"""SELECT
  M.MerchDeptCd AS MerchDeptCd,
  M.MerchDeptDesc AS MerchDeptDesc,
  M.UpdateTstmp AS UpdateTstmp,
  LM4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_MerchClass_4 LM4
  LEFT JOIN MerchDept M ON M.MerchDeptCd = LM4.M.MerchDeptCd"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_MerchDept_5")

# COMMAND ----------
# DBTITLE 1, LKP_GLCategory_6


query_6 = f"""SELECT
  G.GLCategoryCd AS GLCategoryCd,
  G.GLCategoryDesc AS GLCategoryDesc,
  G.UpdateTstmp AS UpdateTstmp,
  SStM3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_MerchCategory_3 SStM3
  LEFT JOIN GLCategory G ON G.GLCategoryCd = SStM3.G.GLCategoryCd"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("LKP_GLCategory_6")

# COMMAND ----------
# DBTITLE 1, LKP_CategoryAnalyst_7


query_7 = f"""SELECT
  C.CategoryAnalystId AS CategoryAnalystId,
  C.CategoryAnalystName AS CategoryAnalystName,
  C.ReplenishmentManagerId AS ReplenishmentManagerId,
  C.UpdateTstmp AS UpdateTstmp,
  SStM3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_MerchCategory_3 SStM3
  LEFT JOIN CategoryAnalyst C ON C.CategoryAnalystId = SStM3.C.CategoryAnalystId"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("LKP_CategoryAnalyst_7")

# COMMAND ----------
# DBTITLE 1, LKP_ReplenishmentManager_8


query_8 = f"""SELECT
  R.ReplenishmentManagerId AS ReplenishmentManagerId,
  R.ReplenishmentManagerName AS ReplenishmentManagerName,
  R.UpdateTstmp AS UpdateTstmp,
  LC7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_CategoryAnalyst_7 LC7
  LEFT JOIN ReplenishmentManager R ON R.ReplenishmentManagerId = LC7.R.ReplenishmentManagerId"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("LKP_ReplenishmentManager_8")

# COMMAND ----------
# DBTITLE 1, EXP_ConvertIDs_9


query_9 = f"""SELECT
  SStM3.MerchCategoryCd AS MerchCategoryCd,
  IFF(
    IS_NUMBER(SStM3.MerchCategoryCd),
    TO_INTEGER(SStM3.MerchCategoryCd),
    -1
  ) AS SAP_Category_ID,
  IFF(
    IS_NUMBER(SStM3.MerchClassCd),
    TO_INTEGER(SStM3.MerchClassCd),
    -1
  ) AS SAP_Class_ID,
  IFF(
    IS_NUMBER(LM5.MerchDeptCd),
    TO_INTEGER(LM5.MerchDeptCd),
    -1
  ) AS SAP_Dept_ID,
  LG6.GLCategoryCd AS GLCategoryCd,
  LG6.GLCategoryDesc AS GLCategoryDesc,
  LC7.CategoryAnalystId AS CategoryAnalystId,
  LC7.CategoryAnalystName AS CategoryAnalystName,
  IFF(
    IS_NUMBER(LR8.ReplenishmentManagerId),
    TO_INTEGER(LR8.ReplenishmentManagerId),
    NULL
  ) AS Replenishment_MGR_ID,
  LR8.ReplenishmentManagerName AS ReplenishmentManagerName,
  LR8.ReplenishmentManagerId AS ReplenishmentManagerId,
  SStM3.MerchCategoryDesc AS MerchCategoryDesc,
  SStM3.MerchClassCd AS MerchClassCd,
  LM4.MerchClassDesc AS MerchClassDesc,
  LM5.MerchDeptCd AS MerchDeptCd,
  LM5.MerchDeptDesc AS MerchDeptDesc,
  LC7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_CategoryAnalyst_7 LC7
  INNER JOIN LKP_ReplenishmentManager_8 LR8 ON LC7.Monotonically_Increasing_Id = LR8.Monotonically_Increasing_Id
  INNER JOIN LKP_GLCategory_6 LG6 ON LR8.Monotonically_Increasing_Id = LG6.Monotonically_Increasing_Id
  INNER JOIN LKP_MerchClass_4 LM4 ON LG6.Monotonically_Increasing_Id = LM4.Monotonically_Increasing_Id
  INNER JOIN LKP_MerchDept_5 LM5 ON LM4.Monotonically_Increasing_Id = LM5.Monotonically_Increasing_Id
  INNER JOIN SQ_Shortcut_to_MerchCategory_3 SStM3 ON LM5.Monotonically_Increasing_Id = SStM3.Monotonically_Increasing_Id"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("EXP_ConvertIDs_9")

# COMMAND ----------
# DBTITLE 1, JNR_GetUpdates_10


query_10 = f"""SELECT
  MASTER.SAP_Category_ID AS SAP_Category_ID,
  MASTER.SAP_Class_ID AS SAP_Class_ID,
  MASTER.SAP_Dept_ID AS SAP_Dept_ID,
  MASTER.GLCategoryCd AS GLCategoryCd,
  MASTER.GLCategoryDesc AS GLCategoryDesc,
  MASTER.CategoryAnalystId AS CategoryAnalystId,
  MASTER.CategoryAnalystName AS CategoryAnalystName,
  MASTER.Replenishment_MGR_ID AS Replenishment_MGR_ID,
  MASTER.ReplenishmentManagerName AS ReplenishmentManagerName,
  DETAIL.SAP_CATEGORY_ID AS SAP_CATEGORY_ID_DST,
  DETAIL.SAP_CLASS_ID AS SAP_CLASS_ID_DST,
  DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID_DST,
  DETAIL.MERCH_GL_CATEGORY_CD AS MERCH_GL_CATEGORY_CD_DST,
  DETAIL.MERCH_GL_CATEGORY_DESC AS MERCH_GL_CATEGORY_DESC_DST,
  DETAIL.CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID_DST,
  DETAIL.CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM_DST,
  DETAIL.REPLENISMENT_MGR_ID AS REPLENISMENT_MGR_ID_DST,
  DETAIL.REPLENISMENT_MGR_NM AS REPLENISMENT_MGR_NM_DST,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_ConvertIDs_9 MASTER
  LEFT JOIN SQ_Shortcut_to_MERCHCAT_ORG_1 DETAIL ON MASTER.SAP_Category_ID = DETAIL.SAP_CATEGORY_ID"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("JNR_GetUpdates_10")

# COMMAND ----------
# DBTITLE 1, EXP_CheckUpdates_11


query_11 = f"""SELECT
  SAP_Category_ID AS SAP_Category_ID,
  SAP_Class_ID AS SAP_Class_ID,
  SAP_Dept_ID AS SAP_Dept_ID,
  GLCategoryCd AS GLCategoryCd,
  GLCategoryDesc AS GLCategoryDesc,
  CategoryAnalystId AS CategoryAnalystId,
  CategoryAnalystName AS CategoryAnalystName,
  Replenishment_MGR_ID AS Replenishment_MGR_ID,
  ReplenishmentManagerName AS ReplenishmentManagerName,
  IFF(
    ISNULL(SAP_CATEGORY_ID_DST),
    'DD_INSERT',
    IFF(
      SAP_Category_ID <> SAP_CATEGORY_ID_DST
      OR SAP_Class_ID <> SAP_CLASS_ID_DST
      OR SAP_Dept_ID <> SAP_DEPT_ID_DST
      OR GLCategoryCd <> MERCH_GL_CATEGORY_CD_DST
      OR GLCategoryDesc <> MERCH_GL_CATEGORY_DESC_DST
      OR IFF(ISNULL(CategoryAnalystId), -1, CategoryAnalystId) <> IFF(
        ISNULL(CATEGORY_ANALYST_ID_DST),
        -1,
        CATEGORY_ANALYST_ID_DST
      )
      OR IFF(
        ISNULL(CategoryAnalystName),
        'NA',
        CategoryAnalystName
      ) <> IFF(
        ISNULL(CATEGORY_ANALYST_NM_DST),
        'NA',
        CATEGORY_ANALYST_NM_DST
      )
      OR IFF(
        ISNULL(Replenishment_MGR_ID),
        -1,
        Replenishment_MGR_ID
      ) <> IFF(
        ISNULL(REPLENISMENT_MGR_ID_DST),
        -1,
        REPLENISMENT_MGR_ID_DST
      )
      OR IFF(
        ISNULL(ReplenishmentManagerName),
        'NA',
        ReplenishmentManagerName
      ) <> IFF(
        ISNULL(REPLENISMENT_MGR_NM_DST),
        'NA',
        REPLENISMENT_MGR_NM_DST
      ),
      'DD_UPDATE',
      'DD_REJECT'
    )
  ) AS UpdateStrategy,
  now() AS UpdateDt,
  LOAD_TSTMP AS LOAD_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP_NN,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_GetUpdates_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("EXP_CheckUpdates_11")

# COMMAND ----------
# DBTITLE 1, FIL_RemoveRejects_12


query_12 = f"""SELECT
  SAP_Category_ID AS SAP_Category_ID,
  SAP_Class_ID AS SAP_Class_ID,
  SAP_Dept_ID AS SAP_Dept_ID,
  GLCategoryCd AS GLCategoryCd,
  GLCategoryDesc AS GLCategoryDesc,
  CategoryAnalystId AS CategoryAnalystId,
  CategoryAnalystName AS CategoryAnalystName,
  Replenishment_MGR_ID AS Replenishment_MGR_ID,
  ReplenishmentManagerName AS ReplenishmentManagerName,
  UpdateStrategy AS UpdateStrategy,
  UpdateDt AS UpdateDt,
  LOAD_TSTMP_NN AS LOAD_TSTMP_NN,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_CheckUpdates_11
WHERE
  UpdateStrategy <> 'DD_REJECT'"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("FIL_RemoveRejects_12")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_13


query_13 = f"""SELECT
  SAP_Category_ID AS SAP_Category_ID,
  SAP_Class_ID AS SAP_Class_ID,
  SAP_Dept_ID AS SAP_Dept_ID,
  GLCategoryCd AS GLCategoryCd,
  GLCategoryDesc AS GLCategoryDesc,
  CategoryAnalystId AS CategoryAnalystId,
  CategoryAnalystName AS CategoryAnalystName,
  Replenishment_MGR_ID AS Replenishment_MGR_ID,
  ReplenishmentManagerName AS ReplenishmentManagerName,
  UpdateStrategy AS UpdateStrategy,
  UpdateDt AS UpdateDt,
  LOAD_TSTMP_NN AS LOAD_TSTMP_NN,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UpdateStrategy AS UPDATE_STRATEGY_FLAG
FROM
  FIL_RemoveRejects_12"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("UPDTRANS_13")

# COMMAND ----------
# DBTITLE 1, MERCHCAT_ORG


spark.sql("""MERGE INTO MERCHCAT_ORG AS TARGET
USING
  UPDTRANS_13 AS SOURCE ON TARGET.SAP_CATEGORY_ID = SOURCE.SAP_Category_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.SAP_CATEGORY_ID = SOURCE.SAP_Category_ID,
  TARGET.SAP_CLASS_ID = SOURCE.SAP_Class_ID,
  TARGET.SAP_DEPT_ID = SOURCE.SAP_Dept_ID,
  TARGET.MERCH_GL_CATEGORY_CD = SOURCE.GLCategoryCd,
  TARGET.MERCH_GL_CATEGORY_DESC = SOURCE.GLCategoryDesc,
  TARGET.CATEGORY_ANALYST_ID = SOURCE.CategoryAnalystId,
  TARGET.CATEGORY_ANALYST_NM = SOURCE.CategoryAnalystName,
  TARGET.CATEGORY_REPLENISHMENT_MGR_ID = SOURCE.Replenishment_MGR_ID,
  TARGET.CATEGORY_REPLENISHMENT_MGR_NM = SOURCE.ReplenishmentManagerName,
  TARGET.UPDATE_TSTMP = SOURCE.UpdateDt,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_NN
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SAP_CLASS_ID = SOURCE.SAP_Class_ID
  AND TARGET.SAP_DEPT_ID = SOURCE.SAP_Dept_ID
  AND TARGET.MERCH_GL_CATEGORY_CD = SOURCE.GLCategoryCd
  AND TARGET.MERCH_GL_CATEGORY_DESC = SOURCE.GLCategoryDesc
  AND TARGET.CATEGORY_ANALYST_ID = SOURCE.CategoryAnalystId
  AND TARGET.CATEGORY_ANALYST_NM = SOURCE.CategoryAnalystName
  AND TARGET.CATEGORY_REPLENISHMENT_MGR_ID = SOURCE.Replenishment_MGR_ID
  AND TARGET.CATEGORY_REPLENISHMENT_MGR_NM = SOURCE.ReplenishmentManagerName
  AND TARGET.UPDATE_TSTMP = SOURCE.UpdateDt
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_NN THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.SAP_CATEGORY_ID,
    TARGET.SAP_CLASS_ID,
    TARGET.SAP_DEPT_ID,
    TARGET.MERCH_GL_CATEGORY_CD,
    TARGET.MERCH_GL_CATEGORY_DESC,
    TARGET.CATEGORY_ANALYST_ID,
    TARGET.CATEGORY_ANALYST_NM,
    TARGET.CATEGORY_REPLENISHMENT_MGR_ID,
    TARGET.CATEGORY_REPLENISHMENT_MGR_NM,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.SAP_Category_ID,
    SOURCE.SAP_Class_ID,
    SOURCE.SAP_Dept_ID,
    SOURCE.GLCategoryCd,
    SOURCE.GLCategoryDesc,
    SOURCE.CategoryAnalystId,
    SOURCE.CategoryAnalystName,
    SOURCE.Replenishment_MGR_ID,
    SOURCE.ReplenishmentManagerName,
    SOURCE.UpdateDt,
    SOURCE.LOAD_TSTMP_NN
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_MerchCat_Org")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_MerchCat_Org", mainWorkflowId, parentName)
