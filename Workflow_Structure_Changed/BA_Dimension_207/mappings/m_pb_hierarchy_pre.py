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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_hierarchy_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_hierarchy_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BrandDirector_0


query_0 = f"""SELECT
  BrandDirectorId AS BrandDirectorId,
  BrandDirectorName AS BrandDirectorName,
  MerchVpId AS MerchVpId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  BrandDirector"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_BrandDirector_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BrandManager_1


query_1 = f"""SELECT
  BrandManagerId AS BrandManagerId,
  BrandManagerName AS BrandManagerName,
  BrandDirectorId AS BrandDirectorId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  BrandManager"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_BrandManager_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DeptBrand_2


query_2 = f"""SELECT
  DeptBrandCd AS DeptBrandCd,
  BrandManagerId AS BrandManagerId,
  BrandCd AS BrandCd,
  MerchDeptCd AS MerchDeptCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  DeptBrand"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_DeptBrand_2")

# COMMAND ----------
# DBTITLE 1, SQ_ADH_BrandHierarchy_3


query_3 = f"""SELECT
  Shortcut_to_BrandDirector_0.BrandDirectorId AS BrandDirectorId,
  Shortcut_to_BrandDirector_0.BrandDirectorName AS BrandDirectorName,
  Shortcut_to_BrandDirector_0.MerchVpId AS MerchVpId,
  Shortcut_to_BrandDirector_0.LoadTstmp AS LoadTstmp1,
  Shortcut_to_BrandDirector_0.UpdateTstmp AS UpdateTstmp1,
  Shortcut_to_BrandManager_1.BrandManagerId AS BrandManagerId,
  Shortcut_to_BrandManager_1.BrandManagerName AS BrandManagerName,
  Shortcut_to_BrandManager_1.BrandDirectorId AS BrandDirectorId1,
  Shortcut_to_BrandManager_1.LoadTstmp AS LoadTstmp2,
  Shortcut_to_BrandManager_1.UpdateTstmp AS UpdateTstmp2,
  Shortcut_to_DeptBrand_2.DeptBrandCd AS DeptBrandCd,
  Shortcut_to_DeptBrand_2.BrandManagerId AS BrandManagerId1,
  Shortcut_to_DeptBrand_2.BrandCd AS BrandCd1,
  Shortcut_to_DeptBrand_2.MerchDeptCd AS MerchDeptCd,
  Shortcut_to_DeptBrand_2.LoadTstmp AS LoadTstmp3,
  Shortcut_to_DeptBrand_2.UpdateTstmp AS UpdateTstmp3,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_BrandDirector_0,
  Shortcut_to_DeptBrand_2,
  Shortcut_to_BrandManager_1
WHERE
  Shortcut_to_DeptBrand_2.BrandManagerId = Shortcut_to_BrandManager_1.BrandManagerId
  AND Shortcut_to_BrandManager_1.BrandDirectorId = Shortcut_to_BrandDirector_0.BrandDirectorId"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_ADH_BrandHierarchy_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ArtAttribute_4


query_4 = f"""SELECT
  ArticleNbr AS ArticleNbr,
  AttTypeID AS AttTypeID,
  AttCodeID AS AttCodeID,
  AttValueID AS AttValueID,
  DeleteFlag AS DeleteFlag,
  DeleteTstmp AS DeleteTstmp,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp
FROM
  ArtAttribute"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_ArtAttribute_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ArtAttribute_5


query_5 = f"""SELECT
  ArticleNbr AS ArticleNbr,
  AttTypeID AS AttTypeID,
  AttCodeID AS AttCodeID,
  AttValueID AS AttValueID,
  DeleteFlag AS DeleteFlag,
  DeleteTstmp AS DeleteTstmp,
  UpdateTstmp AS UpdateTstmp,
  LoadTstmp AS LoadTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ArtAttribute_4
WHERE
  Shortcut_to_ArtAttribute_4.AttCodeID = 'PBRD'
  and Shortcut_to_ArtAttribute_4.AttValueID = 'NBE'"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_ArtAttribute_5")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Brand1_6


query_6 = f"""SELECT
  BrandCd AS BrandCd,
  BrandName AS BrandName,
  BrandTypeCd AS BrandTypeCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  BrandClassificationCd AS BrandClassificationCd
FROM
  Brand"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Shortcut_to_Brand1_6")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ArtMas_7


query_7 = f"""SELECT
  ArticleNbr AS ArticleNbr,
  ArticleDesc AS ArticleDesc,
  AlternateDesc AS AlternateDesc,
  ArticleTypeCd AS ArticleTypeCd,
  MerchCategoryCd AS MerchCategoryCd,
  ArticleCategoryCd AS ArticleCategoryCd,
  ArticleStatusCd AS ArticleStatusCd,
  PrimaryVendorCd AS PrimaryVendorCd,
  BrandCd AS BrandCd,
  ProcRule AS ProcRule,
  FlavorCd AS FlavorCd,
  ColorCd AS ColorCd,
  SizeCd AS SizeCd,
  RtvCd AS RtvCd,
  CreateDt AS CreateDt,
  CreatedBy AS CreatedBy,
  UpdateDt AS UpdateDt,
  UpdatedBy AS UpdatedBy,
  BaseUomCd AS BaseUomCd,
  BaseUomIsoCd AS BaseUomIsoCd,
  DocNbr AS DocNbr,
  DocSheetCnt AS DocSheetCnt,
  WeightNetAmt AS WeightNetAmt,
  ContainerReqmtCd AS ContainerReqmtCd,
  TransportGroupCd AS TransportGroupCd,
  DivisionCd AS DivisionCd,
  GrGiSlipPrintCnt AS GrGiSlipPrintCnt,
  SupplySourceCd AS SupplySourceCd,
  WeightAllowedPkgAmt AS WeightAllowedPkgAmt,
  VolumeAllowedPkgAmt AS VolumeAllowedPkgAmt,
  WeightToleranceAmt AS WeightToleranceAmt,
  VolumeToleranceAmt AS VolumeToleranceAmt,
  VariableOrderUnitFlag AS VariableOrderUnitFlag,
  VolumeFillAmt AS VolumeFillAmt,
  StackingFactorAmt AS StackingFactorAmt,
  ShelfLifeRemCnt AS ShelfLifeRemCnt,
  ShelfLifeTotalCnt AS ShelfLifeTotalCnt,
  StoragePct AS StoragePct,
  ValidFromDt AS ValidFromDt,
  DeleteDt AS DeleteDt,
  XSiteStatusCd AS XSiteStatusCd,
  XSiteValidFromDt AS XSiteValidFromDt,
  XDistValidFromDt AS XDistValidFromDt,
  TaxClassCd AS TaxClassCd,
  ContentUnitCd AS ContentUnitCd,
  NetContentsAmt AS NetContentsAmt,
  ContentMetricUnitCd AS ContentMetricUnitCd,
  NetContentsMetricAmt AS NetContentsMetricAmt,
  CompPriceUnitAmt AS CompPriceUnitAmt,
  GrossContentsAmt AS GrossContentsAmt,
  ItemCategory AS ItemCategory,
  FiberShare1Pct AS FiberShare1Pct,
  FiberShare2Pct AS FiberShare2Pct,
  FiberShare3Pct AS FiberShare3Pct,
  FiberShare4Pct AS FiberShare4Pct,
  FiberShare5Pct AS FiberShare5Pct,
  TempSKU AS TempSKU,
  CopySKU AS CopySKU,
  OldArticleNbr AS OldArticleNbr,
  MandatorySkuFlag AS MandatorySkuFlag,
  BasicMaterial AS BasicMaterial,
  RxFlag AS RxFlag,
  Seasonality AS Seasonality,
  iDocNumber AS iDocNumber,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  ArtMas"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Shortcut_to_ArtMas_7")

# COMMAND ----------
# DBTITLE 1, SQ_ArtMast_Brand_8


query_8 = f"""SELECT
  Shortcut_to_ArtMas_7.ArticleNbr AS ArticleNbr,
  Shortcut_to_ArtMas_7.BrandCd AS BrandCd,
  Shortcut_to_Brand1_6.BrandCd AS BrandCd1,
  Shortcut_to_Brand1_6.BrandClassificationCd AS BrandClassificationCd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ArtMas_7,
  Shortcut_to_Brand1_6
WHERE
  Shortcut_to_ArtMas_7.BrandCd = Shortcut_to_Brand1_6.BrandCd
  AND Shortcut_to_Brand1_6.BrandClassificationCd is not null"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("SQ_ArtMast_Brand_8")

# COMMAND ----------
# DBTITLE 1, Exp_LPAD_ArtAttibute_9


query_9 = f"""SELECT
  LPAD(ArticleNbr, 18, '0') AS ArticleNbr1,
  BrandCd AS BrandCd,
  BrandCd1 AS BrandCd1,
  BrandClassificationCd AS BrandClassificationCd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_ArtMast_Brand_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("Exp_LPAD_ArtAttibute_9")

# COMMAND ----------
# DBTITLE 1, Jnr_Brand_Art_10


query_10 = f"""SELECT
  MASTER.ArticleNbr1 AS ArticleNbr,
  MASTER.BrandCd AS BrandCd,
  MASTER.BrandCd1 AS BrandCd1,
  MASTER.BrandClassificationCd AS BrandClassificationCd,
  DETAIL.ArticleNbr AS ArticleNbr1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_LPAD_ArtAttibute_9 MASTER
  LEFT JOIN SQ_Shortcut_to_ArtAttribute_5 DETAIL ON MASTER.ArticleNbr1 = DETAIL.ArticleNbr"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("Jnr_Brand_Art_10")

# COMMAND ----------
# DBTITLE 1, Exp_Brand_Classification_11


query_11 = f"""SELECT
  BrandCd AS BRAND_CD,
  IFF(ISNULL(ArticleNbr1), BrandClassificationCd, 5) AS BRAND_CLASSIFICATION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_Brand_Art_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("Exp_Brand_Classification_11")

# COMMAND ----------
# DBTITLE 1, SRTTRANS_12


query_12 = f"""SELECT
  BRAND_CD AS BrandCd,
  BRAND_CLASSIFICATION_ID AS BrandClassificationCd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_Brand_Classification_11
ORDER BY
  BrandCd ASC,
  BrandClassificationCd ASC"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("SRTTRANS_12")

# COMMAND ----------
# DBTITLE 1, Jnr_ADH_EDW__Brand_13


query_13 = f"""SELECT
  DETAIL.BrandManagerId AS BrandManagerId,
  DETAIL.BrandDirectorId1 AS BrandDirectorId1,
  DETAIL.MerchDeptCd AS MerchDeptCd,
  DETAIL.BrandCd1 AS BrandCd1,
  MASTER.BrandCd AS BRAND_CD,
  MASTER.BrandClassificationCd AS BRAND_CLASSIFICATION_ID,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SRTTRANS_12 MASTER
  INNER JOIN SQ_ADH_BrandHierarchy_3 DETAIL ON MASTER.BrandCd = DETAIL.BrandCd1"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("Jnr_ADH_EDW__Brand_13")

# COMMAND ----------
# DBTITLE 1, PB_HIERARCHY_PRE


spark.sql("""INSERT INTO
  PB_HIERARCHY_PRE
SELECT
  BRAND_CD AS BRAND_CD,
  MerchDeptCd AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  BrandManagerId AS PB_MANAGER_ID,
  BrandDirectorId1 AS PB_DIRECTOR_ID
FROM
  Jnr_ADH_EDW__Brand_13""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_hierarchy_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_hierarchy_pre", mainWorkflowId, parentName)
