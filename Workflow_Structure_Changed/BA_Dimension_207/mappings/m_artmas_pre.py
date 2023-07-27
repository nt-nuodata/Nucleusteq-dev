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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_artmas_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_artmas_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ArtMas_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ArtMas_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ArtMas_1


query_1 = f"""SELECT
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
  BasicMaterial AS BasicMaterial,
  RxFlag AS RxFlag,
  Seasonality AS Seasonality,
  iDocNumber AS iDocNumber,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ArtMas_0
WHERE
  NOT Shortcut_to_ArtMas_0.ArticleNbr = 'NaN'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ArtMas_1")

# COMMAND ----------
# DBTITLE 1, ARTMAS_PRE


spark.sql("""INSERT INTO
  ARTMAS_PRE
SELECT
  ArticleNbr AS ARTICLE_NBR,
  ArticleDesc AS ARTICLE_DESC,
  AlternateDesc AS ALTERNATE_DESC,
  ArticleTypeCd AS ARTICLE_TYPE_CD,
  MerchCategoryCd AS MERCH_CATEGORY_CD,
  ArticleCategoryCd AS ARTICLE_CATEGORY_CD,
  ArticleStatusCd AS ARTICLE_STATUS_CD,
  PrimaryVendorCd AS PRIMARY_VENDOR_CD,
  BrandCd AS BRAND_CD,
  ProcRule AS PROC_RULE,
  FlavorCd AS FLAVOR_CD,
  ColorCd AS COLOR_CD,
  SizeCd AS SIZE_CD,
  RtvCd AS RTV_CD,
  CreateDt AS CREATE_DT,
  CreatedBy AS CREATED_BY,
  UpdateDt AS UPDATE_DT,
  UpdatedBy AS UPDATED_BY,
  BaseUomCd AS BASE_UOM_CD,
  BaseUomIsoCd AS BASE_UOM_ISO_CD,
  DocNbr AS DOC_NBR,
  DocSheetCnt AS DOC_SHEET_CNT,
  WeightNetAmt AS WEIGHT_NET_AMT,
  ContainerReqmtCd AS CONTAINER_REQMT_CD,
  TransportGroupCd AS TRANSPORT_GROUP_CD,
  DivisionCd AS DIVISION_CD,
  GrGiSlipPrintCnt AS GR_GI_SLIP_PRINT_CNT,
  SupplySourceCd AS SUPPLY_SOURCE_CD,
  WeightAllowedPkgAmt AS WEIGHT_ALLOWED_PKG_AMT,
  VolumeAllowedPkgAmt AS VOLUME_ALLOWED_PKG_AMT,
  WeightToleranceAmt AS WEIGHT_TOLERANCE_AMT,
  VolumeToleranceAmt AS VOLUME_TOLERANCE_AMT,
  VariableOrderUnitFlag AS VARIABLE_ORDER_UNIT_FLAG,
  VolumeFillAmt AS VOLUME_FILL_AMT,
  StackingFactorAmt AS STACKING_FACTOR_AMT,
  ShelfLifeRemCnt AS SHELF_LIFE_REM_CNT,
  ShelfLifeTotalCnt AS SHELF_LIFE_TOTAL_CNT,
  StoragePct AS STORAGE_PCT,
  ValidFromDt AS VALID_FROM_DT,
  DeleteDt AS DELETE_DT,
  XSiteStatusCd AS XSITE_STATUS_CD,
  XSiteValidFromDt AS XSITE_VALID_FROM_DT,
  XDistValidFromDt AS XDIST_VALID_FROM_DT,
  TaxClassCd AS TAX_CLASS_CD,
  ContentUnitCd AS CONTENT_UNIT_CD,
  NetContentsAmt AS NET_CONTENTS_AMT,
  ContentMetricUnitCd AS CONTENT_METRIC_UNIT_CD,
  NetContentsMetricAmt AS NET_CONTENTS_METRIC_AMT,
  CompPriceUnitAmt AS COMP_PRICE_UNIT_AMT,
  GrossContentsAmt AS GROSS_CONTENTS_AMT,
  ItemCategory AS ITEM_CATEGORY,
  FiberShare1Pct AS FIBER_SHARE1_PCT,
  FiberShare2Pct AS FIBER_SHARE2_PCT,
  FiberShare3Pct AS FIBER_SHARE3_PCT,
  FiberShare4Pct AS FIBER_SHARE4_PCT,
  FiberShare5Pct AS FIBER_SHARE5_PCT,
  TempSKU AS TEMP_SKU,
  CopySKU AS COPY_SKU,
  OldArticleNbr AS OLD_ARTICLE_NBR,
  BasicMaterial AS BASIC_MATERIAL,
  RxFlag AS RX_FLAG,
  Seasonality AS SEASONALITY,
  iDocNumber AS IDOC_NUMBER,
  LoadTstmp AS LOAD_TSTMP,
  UpdateTstmp AS UPDATE_TSTMP
FROM
  SQ_Shortcut_to_ArtMas_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_artmas_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_artmas_pre", mainWorkflowId, parentName)
