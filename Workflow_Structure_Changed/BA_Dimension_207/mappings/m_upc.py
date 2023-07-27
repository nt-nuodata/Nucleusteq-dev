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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_upc")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_upc", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UPC_Flat_0


query_0 = f"""SELECT
  UPC_ID AS UPC_ID,
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR
FROM
  UPC_Flat"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UPC_Flat_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UPC_Flat_1


query_1 = f"""SELECT
  UPC_ID AS UPC_ID,
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UPC_Flat_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UPC_Flat_1")

# COMMAND ----------
# DBTITLE 1, LKP_UPC_2


query_2 = f"""SELECT
  SStUF1.UPC_ID AS in_UPC_ID,
  U.UPC_ID AS UPC_ID,
  U.UPC_ADD_DT AS UPC_ADD_DT,
  U.UPC_DELETE_DT AS UPC_DELETE_DT,
  U.UPC_REFRESH_DT AS UPC_REFRESH_DT,
  U.PRODUCT_ID AS PRODUCT_ID,
  U.SKU_NBR AS SKU_NBR,
  SStUF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UPC_Flat_1 SStUF1
  LEFT JOIN UPC U ON U.UPC_ID = SStUF1.U.UPC_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_UPC_2")

# COMMAND ----------
# DBTITLE 1, EXP_UPC_3


query_3 = f"""SELECT
  TRUNC(SESSSTARTTIME) AS DATE_ADDED_REFRESHED_VAR,
  LU2.UPC_ID AS UPC_ID_FROM_LKP,
  SStUF1.UPC_ID AS UPC_ID,
  IFF(LU2.UPC_ID > 0, LU2.UPC_ADD_DT, TRUNC(SESSSTARTTIME)) AS DATE_UPC_ADDED,
  TO_DATE('12-31-9999', 'mm-dd-yyyy') AS DATE_UPC_DELETED,
  TRUNC(SESSSTARTTIME) AS DATE_UPC_REFRESHED,
  SStUF1.PRODUCT_ID AS PRODUCT_ID,
  SStUF1.SKU_NBR AS SKU_NBR,
  SStUF1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UPC_Flat_1 SStUF1
  INNER JOIN LKP_UPC_2 LU2 ON SStUF1.Monotonically_Increasing_Id = LU2.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_UPC_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_EXP_UPC_CD_4


query_4 = f"""SELECT
  IFF(
    LENGTH(UPC_ID) <= 7,
    LPAD(TO_CHAR(UPC_ID), 7, '0'),
    IFF(
      LENGTH(UPC_ID) <= 11,
      LPAD(TO_CHAR(UPC_ID), 11, '0'),
      TO_CHAR(UPC_ID)
    )
  ) AS UPC_TXT_RAW,
  SUBSTR(UPC_TXT_RAW, 2, 1) AS RAW1,
  SUBSTR(UPC_TXT_RAW, 3, 1) AS RAW2,
  SUBSTR(UPC_TXT_RAW, 4, 1) AS RAW3,
  SUBSTR(UPC_TXT_RAW, 5, 1) AS RAW4,
  SUBSTR(UPC_TXT_RAW, 6, 1) AS RAW5,
  SUBSTR(UPC_TXT_RAW, 7, 1) AS RAW6,
  IFF(
    LENGTH(UPC_TXT_RAW) = 7,
    IFF(
      RAW6 = '0',
      '0' || RAW1 || RAW2 || '00000' || RAW3 || RAW4 || RAW5,
      IFF(
        RAW6 = '1',
        '0' || RAW1 || RAW2 || '10000' || RAW3 || RAW4 || RAW5,
        IFF(
          RAW6 = '2',
          '0' || RAW1 || RAW2 || '20000' || RAW3 || RAW4 || RAW5,
          IFF(
            RAW6 = '3',
            '0' || RAW1 || RAW2 || RAW3 || '00000' || RAW4 || RAW5,
            IFF(
              RAW6 = '4',
              '0' || RAW1 || RAW2 || RAW3 || RAW4 || '00000' || RAW5,
              IFF(
                RAW6 = '5',
                '0' || RAW1 || RAW2 || RAW3 || RAW4 || RAW5 || '00005',
                IFF(
                  RAW6 = '6',
                  '0' || RAW1 || RAW2 || RAW3 || RAW4 || RAW5 || '00006',
                  IFF(
                    RAW6 = '7',
                    '0' || RAW1 || RAW2 || RAW3 || RAW4 || RAW5 || '00007',
                    IFF(
                      RAW6 = '8',
                      '0' || RAW1 || RAW2 || RAW3 || RAW4 || RAW5 || '00008',
                      IFF(
                        RAW6 = '9',
                        '0' || RAW1 || RAW2 || RAW3 || RAW4 || RAW5 || '00009'
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    ),
    UPC_TXT_RAW
  ) AS UPC_TXT,
  TO_INTEGER(SUBSTR(UPC_TXT, 1, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 3, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 5, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 7, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 9, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 11, 1)) AS ODD_SUM,
  TO_INTEGER(SUBSTR(UPC_TXT, 2, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 4, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 6, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 8, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 10, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 12, 1)) AS EVEN_SUM,
  ODD_SUM * 3 AS ODD_TIMES_3,
  EVEN_SUM * 3 AS EVEN_TIMES_3,
  ODD_TIMES_3 + EVEN_SUM AS ODD_TIMES_3_PLUS_EVEN,
  EVEN_TIMES_3 + ODD_SUM AS EVEN_TIMES_3_PLUS_ODD,
  IFF(
    LENGTH(UPC_TXT) < 12,
    SUBSTR(
      ODD_TIMES_3_PLUS_EVEN,
      LENGTH(ODD_TIMES_3_PLUS_EVEN),
      1
    ),
    SUBSTR(
      EVEN_TIMES_3_PLUS_ODD,
      LENGTH(EVEN_TIMES_3_PLUS_ODD),
      1
    )
  ) AS REMAINDER,
  IFF(REMAINDER = 0, '0', TO_CHAR(10 - REMAINDER)) AS CHECK_DIGIT,
  IFF(
    LENGTH(UPC_ID) <= 7,
    LPAD(TO_CHAR(UPC_ID), 7, '0'),
    IFF(
      LENGTH(UPC_ID) <= 11,
      LPAD(TO_CHAR(UPC_ID), 11, '0'),
      TO_CHAR(UPC_ID)
    )
  ) || IFF(
    IFF(
      LENGTH(
        IFF(
          LENGTH(
            IFF(
              LENGTH(UPC_ID) <= 7,
              LPAD(TO_CHAR(UPC_ID), 7, '0'),
              IFF(
                LENGTH(UPC_ID) <= 11,
                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                TO_CHAR(UPC_ID)
              )
            )
          ) = 7,
          IFF(
            SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              7,
              1
            ) = '0',
            '0' || SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              2,
              1
            ) || SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              3,
              1
            ) || '00000' || SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              4,
              1
            ) || SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              5,
              1
            ) || SUBSTR(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              ),
              6,
              1
            ),
            IFF(
              SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                7,
                1
              ) = '1',
              '0' || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                2,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                3,
                1
              ) || '10000' || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                4,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                5,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                6,
                1
              ),
              IFF(
                SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  7,
                  1
                ) = '2',
                '0' || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  2,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  3,
                  1
                ) || '20000' || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  4,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  5,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  6,
                  1
                ),
                IFF(
                  SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    7,
                    1
                  ) = '3',
                  '0' || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    2,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    3,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    4,
                    1
                  ) || '00000' || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    5,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    6,
                    1
                  ),
                  IFF(
                    SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      7,
                      1
                    ) = '4',
                    '0' || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      2,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      3,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      4,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      5,
                      1
                    ) || '00000' || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      6,
                      1
                    ),
                    IFF(
                      SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        7,
                        1
                      ) = '5',
                      '0' || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        2,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        3,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        4,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        5,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        6,
                        1
                      ) || '00005',
                      IFF(
                        SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          7,
                          1
                        ) = '6',
                        '0' || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          2,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          3,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          4,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          5,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          6,
                          1
                        ) || '00006',
                        IFF(
                          SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            7,
                            1
                          ) = '7',
                          '0' || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            2,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            3,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            4,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            5,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            6,
                            1
                          ) || '00007',
                          IFF(
                            SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              7,
                              1
                            ) = '8',
                            '0' || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              2,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              3,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              4,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              5,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              6,
                              1
                            ) || '00008',
                            IFF(
                              SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                7,
                                1
                              ) = '9',
                              '0' || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                2,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                3,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                4,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                5,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                6,
                                1
                              ) || '00009'
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          ),
          IFF(
            LENGTH(UPC_ID) <= 7,
            LPAD(TO_CHAR(UPC_ID), 7, '0'),
            IFF(
              LENGTH(UPC_ID) <= 11,
              LPAD(TO_CHAR(UPC_ID), 11, '0'),
              TO_CHAR(UPC_ID)
            )
          )
        )
      ) < 12,
      SUBSTR(ODD_SUM * 3 + EVEN_SUM, LENGTH(ODD_SUM * 3 + EVEN_SUM), 1),
      SUBSTR(
        TO_INTEGER(SUBSTR(UPC_TXT, 2, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 4, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 6, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 8, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 10, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 12, 1)) * 3 + TO_INTEGER(SUBSTR(UPC_TXT, 1, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 3, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 5, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 7, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 9, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 11, 1)),
        LENGTH(
          TO_INTEGER(SUBSTR(UPC_TXT, 2, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 4, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 6, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 8, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 10, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 12, 1)) * 3 + TO_INTEGER(SUBSTR(UPC_TXT, 1, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 3, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 5, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 7, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 9, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 11, 1))
        ),
        1
      )
    ) = 0,
    '0',
    TO_CHAR(
      10 - IFF(
        LENGTH(
          IFF(
            LENGTH(
              IFF(
                LENGTH(UPC_ID) <= 7,
                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                IFF(
                  LENGTH(UPC_ID) <= 11,
                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                  TO_CHAR(UPC_ID)
                )
              )
            ) = 7,
            IFF(
              SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                7,
                1
              ) = '0',
              '0' || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                2,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                3,
                1
              ) || '00000' || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                4,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                5,
                1
              ) || SUBSTR(
                IFF(
                  LENGTH(UPC_ID) <= 7,
                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                  IFF(
                    LENGTH(UPC_ID) <= 11,
                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                    TO_CHAR(UPC_ID)
                  )
                ),
                6,
                1
              ),
              IFF(
                SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  7,
                  1
                ) = '1',
                '0' || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  2,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  3,
                  1
                ) || '10000' || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  4,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  5,
                  1
                ) || SUBSTR(
                  IFF(
                    LENGTH(UPC_ID) <= 7,
                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                    IFF(
                      LENGTH(UPC_ID) <= 11,
                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                      TO_CHAR(UPC_ID)
                    )
                  ),
                  6,
                  1
                ),
                IFF(
                  SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    7,
                    1
                  ) = '2',
                  '0' || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    2,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    3,
                    1
                  ) || '20000' || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    4,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    5,
                    1
                  ) || SUBSTR(
                    IFF(
                      LENGTH(UPC_ID) <= 7,
                      LPAD(TO_CHAR(UPC_ID), 7, '0'),
                      IFF(
                        LENGTH(UPC_ID) <= 11,
                        LPAD(TO_CHAR(UPC_ID), 11, '0'),
                        TO_CHAR(UPC_ID)
                      )
                    ),
                    6,
                    1
                  ),
                  IFF(
                    SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      7,
                      1
                    ) = '3',
                    '0' || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      2,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      3,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      4,
                      1
                    ) || '00000' || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      5,
                      1
                    ) || SUBSTR(
                      IFF(
                        LENGTH(UPC_ID) <= 7,
                        LPAD(TO_CHAR(UPC_ID), 7, '0'),
                        IFF(
                          LENGTH(UPC_ID) <= 11,
                          LPAD(TO_CHAR(UPC_ID), 11, '0'),
                          TO_CHAR(UPC_ID)
                        )
                      ),
                      6,
                      1
                    ),
                    IFF(
                      SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        7,
                        1
                      ) = '4',
                      '0' || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        2,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        3,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        4,
                        1
                      ) || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        5,
                        1
                      ) || '00000' || SUBSTR(
                        IFF(
                          LENGTH(UPC_ID) <= 7,
                          LPAD(TO_CHAR(UPC_ID), 7, '0'),
                          IFF(
                            LENGTH(UPC_ID) <= 11,
                            LPAD(TO_CHAR(UPC_ID), 11, '0'),
                            TO_CHAR(UPC_ID)
                          )
                        ),
                        6,
                        1
                      ),
                      IFF(
                        SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          7,
                          1
                        ) = '5',
                        '0' || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          2,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          3,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          4,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          5,
                          1
                        ) || SUBSTR(
                          IFF(
                            LENGTH(UPC_ID) <= 7,
                            LPAD(TO_CHAR(UPC_ID), 7, '0'),
                            IFF(
                              LENGTH(UPC_ID) <= 11,
                              LPAD(TO_CHAR(UPC_ID), 11, '0'),
                              TO_CHAR(UPC_ID)
                            )
                          ),
                          6,
                          1
                        ) || '00005',
                        IFF(
                          SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            7,
                            1
                          ) = '6',
                          '0' || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            2,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            3,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            4,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            5,
                            1
                          ) || SUBSTR(
                            IFF(
                              LENGTH(UPC_ID) <= 7,
                              LPAD(TO_CHAR(UPC_ID), 7, '0'),
                              IFF(
                                LENGTH(UPC_ID) <= 11,
                                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                TO_CHAR(UPC_ID)
                              )
                            ),
                            6,
                            1
                          ) || '00006',
                          IFF(
                            SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              7,
                              1
                            ) = '7',
                            '0' || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              2,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              3,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              4,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              5,
                              1
                            ) || SUBSTR(
                              IFF(
                                LENGTH(UPC_ID) <= 7,
                                LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                IFF(
                                  LENGTH(UPC_ID) <= 11,
                                  LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                  TO_CHAR(UPC_ID)
                                )
                              ),
                              6,
                              1
                            ) || '00007',
                            IFF(
                              SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                7,
                                1
                              ) = '8',
                              '0' || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                2,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                3,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                4,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                5,
                                1
                              ) || SUBSTR(
                                IFF(
                                  LENGTH(UPC_ID) <= 7,
                                  LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                  IFF(
                                    LENGTH(UPC_ID) <= 11,
                                    LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                    TO_CHAR(UPC_ID)
                                  )
                                ),
                                6,
                                1
                              ) || '00008',
                              IFF(
                                SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  7,
                                  1
                                ) = '9',
                                '0' || SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  2,
                                  1
                                ) || SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  3,
                                  1
                                ) || SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  4,
                                  1
                                ) || SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  5,
                                  1
                                ) || SUBSTR(
                                  IFF(
                                    LENGTH(UPC_ID) <= 7,
                                    LPAD(TO_CHAR(UPC_ID), 7, '0'),
                                    IFF(
                                      LENGTH(UPC_ID) <= 11,
                                      LPAD(TO_CHAR(UPC_ID), 11, '0'),
                                      TO_CHAR(UPC_ID)
                                    )
                                  ),
                                  6,
                                  1
                                ) || '00009'
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            ),
            IFF(
              LENGTH(UPC_ID) <= 7,
              LPAD(TO_CHAR(UPC_ID), 7, '0'),
              IFF(
                LENGTH(UPC_ID) <= 11,
                LPAD(TO_CHAR(UPC_ID), 11, '0'),
                TO_CHAR(UPC_ID)
              )
            )
          )
        ) < 12,
        SUBSTR(ODD_SUM * 3 + EVEN_SUM, LENGTH(ODD_SUM * 3 + EVEN_SUM), 1),
        SUBSTR(
          TO_INTEGER(SUBSTR(UPC_TXT, 2, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 4, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 6, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 8, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 10, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 12, 1)) * 3 + TO_INTEGER(SUBSTR(UPC_TXT, 1, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 3, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 5, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 7, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 9, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 11, 1)),
          LENGTH(
            TO_INTEGER(SUBSTR(UPC_TXT, 2, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 4, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 6, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 8, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 10, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 12, 1)) * 3 + TO_INTEGER(SUBSTR(UPC_TXT, 1, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 3, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 5, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 7, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 9, 1)) + TO_INTEGER(SUBSTR(UPC_TXT, 11, 1))
          ),
          1
        )
      )
    )
  ) AS UPC_CD,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UPC_Flat_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_EXP_UPC_CD_4")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_5


query_5 = f"""SELECT
  EU3.UPC_ID_FROM_LKP AS UPC_ID_FROM_LKP,
  EU3.UPC_ID AS UPC_ID,
  StEUC4.UPC_CD AS UPC_CD,
  EU3.DATE_UPC_ADDED AS UPC_ADD_DT,
  EU3.DATE_UPC_DELETED AS UPC_DELETE_DT,
  EU3.DATE_UPC_REFRESHED AS UPC_REFRESH_DT,
  EU3.PRODUCT_ID AS PRODUCT_ID,
  EU3.SKU_NBR AS SKU_NBR,
  StEUC4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(EU3.UPC_ID_FROM_LKP > 0, 'DD_UPDATE', 'DD_INSERT') AS UPDATE_STRATEGY_FLAG
FROM
  Shortcut_to_EXP_UPC_CD_4 StEUC4
  INNER JOIN EXP_UPC_3 EU3 ON StEUC4.Monotonically_Increasing_Id = EU3.Monotonically_Increasing_Id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("UPDTRANS_5")

# COMMAND ----------
# DBTITLE 1, UPC


spark.sql("""MERGE INTO UPC AS TARGET
USING
  UPDTRANS_5 AS SOURCE ON TARGET.UPC_ID = SOURCE.UPC_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.UPC_ID = SOURCE.UPC_ID,
  TARGET.UPC_CD = SOURCE.UPC_CD,
  TARGET.UPC_ADD_DT = SOURCE.UPC_ADD_DT,
  TARGET.UPC_DELETE_DT = SOURCE.UPC_DELETE_DT,
  TARGET.UPC_REFRESH_DT = SOURCE.UPC_REFRESH_DT,
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.SKU_NBR = SOURCE.SKU_NBR
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.UPC_CD = SOURCE.UPC_CD
  AND TARGET.UPC_ADD_DT = SOURCE.UPC_ADD_DT
  AND TARGET.UPC_DELETE_DT = SOURCE.UPC_DELETE_DT
  AND TARGET.UPC_REFRESH_DT = SOURCE.UPC_REFRESH_DT
  AND TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  AND TARGET.SKU_NBR = SOURCE.SKU_NBR THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.UPC_ID,
    TARGET.UPC_CD,
    TARGET.UPC_ADD_DT,
    TARGET.UPC_DELETE_DT,
    TARGET.UPC_REFRESH_DT,
    TARGET.PRODUCT_ID,
    TARGET.SKU_NBR
  )
VALUES
  (
    SOURCE.UPC_ID,
    SOURCE.UPC_CD,
    SOURCE.UPC_ADD_DT,
    SOURCE.UPC_DELETE_DT,
    SOURCE.UPC_REFRESH_DT,
    SOURCE.PRODUCT_ID,
    SOURCE.SKU_NBR
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_upc")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_upc", mainWorkflowId, parentName)
