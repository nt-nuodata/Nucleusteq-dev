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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_brand_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_brand_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ArtAttribute_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ArtAttribute_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ArtAttribute_1


query_1 = f"""SELECT
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
  Shortcut_to_ArtAttribute_0
WHERE
  Shortcut_to_ArtAttribute_0.AttCodeID = 'PBRD'
  and Shortcut_to_ArtAttribute_0.AttValueID = 'NBE'
  and Shortcut_to_ArtAttribute_0.DeleteFlag = 0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ArtAttribute_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Brand_2


query_2 = f"""SELECT
  BrandCd AS BrandCd,
  BrandName AS BrandName,
  BrandTypeCd AS BrandTypeCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  BrandClassificationCd AS BrandClassificationCd
FROM
  Brand"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_Brand_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ArtMas_3


query_3 = f"""SELECT
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

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_ArtMas_3")

# COMMAND ----------
# DBTITLE 1, SQ_ArtMast_Brand_4


query_4 = f"""SELECT
  Shortcut_to_ArtMas_3.ArticleNbr AS ArticleNbr,
  Shortcut_to_ArtMas_3.BrandCd AS BrandCd,
  Shortcut_to_Brand_2.BrandCd AS BrandCd1,
  Shortcut_to_Brand_2.BrandClassificationCd AS BrandClassificationCd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Brand_2,
  Shortcut_to_ArtMas_3
WHERE
  Shortcut_to_ArtMas_3.BrandCd = Shortcut_to_Brand_2.BrandCd"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_ArtMast_Brand_4")

# COMMAND ----------
# DBTITLE 1, EXPTRANS1_5


query_5 = f"""SELECT
  LPAD(ArticleNbr, 18, '0') AS ArticleNbr1,
  BrandCd AS BrandCd,
  BrandCd1 AS BrandCd1,
  BrandClassificationCd AS BrandClassificationCd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_ArtMast_Brand_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXPTRANS1_5")

# COMMAND ----------
# DBTITLE 1, JNRTRANS_6


query_6 = f"""SELECT
  MASTER.ArticleNbr1 AS ArticleNbr,
  MASTER.BrandCd AS BrandCd,
  MASTER.BrandCd1 AS BrandCd1,
  MASTER.BrandClassificationCd AS BrandClassificationCd,
  DETAIL.ArticleNbr AS ArticleNbr1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXPTRANS1_5 MASTER
  LEFT JOIN SQ_Shortcut_to_ArtAttribute_1 DETAIL ON MASTER.ArticleNbr1 = DETAIL.ArticleNbr"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("JNRTRANS_6")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_7


query_7 = f"""SELECT
  ArticleNbr AS SKU_NBR,
  BrandCd AS BRAND_CD,
  IFF(ISNULL(ArticleNbr1), BrandClassificationCd, 5) AS BRAND_CLASSIFICATION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNRTRANS_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXPTRANS_7")

# COMMAND ----------
# DBTITLE 1, SKU_BRAND_PRE


spark.sql("""INSERT INTO
  SKU_BRAND_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  BRAND_CD AS BRAND_CD,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID
FROM
  EXPTRANS_7""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_brand_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_brand_pre", mainWorkflowId, parentName)
