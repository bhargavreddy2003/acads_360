# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, when, sum as spark_sum, count

# COMMAND ----------

# List files in the Filestore directory
files = dbutils.fs.ls("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/")
for file in files:
    print(file.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GRN DATA

# COMMAND ----------

df_grn = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("useHeader", "true") \
    .load("/FileStore/tables/GRN_Data_Analysis.csv")

df_grn.display()

# COMMAND ----------

df_grn= df_grn.withColumnRenamed("Po Number", "PO_NUMBER")
df_grn=df_grn.drop(df_grn.columns[-1])
df_grn.display()

# COMMAND ----------

df_grn.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## PO DATA

# COMMAND ----------



# COMMAND ----------

df_PO = spark.read.format("csv")\
    .option("inferSchema","True")\
    .option("header", "True")\
    .option("delimater", ",")\
    .load("/FileStore/tables/Neuland_PO_data_for_Analytics.csv")
    
df_PO.display()

# COMMAND ----------

df_PO.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 TO 1 Mapping

# COMMAND ----------

windowSpec = Window.partitionBy("PO_NUMBER").orderBy("PO_NUMBER")

# Add row numbers to both DataFrames
df_grn_numbered = df_grn.withColumn("row_num", row_number().over(windowSpec))
df_PO_numbered = df_PO.withColumn("row_num", row_number().over(windowSpec))

# COMMAND ----------

df_PO_numbered.display()
df_grn_numbered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SELECTING ALL NEEDED COLUMNS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining both tables
# MAGIC

# COMMAND ----------

df_joined = df_grn_numbered.join(df_PO_numbered, on=["PO_NUMBER", "row_num"], how="inner")
df_joined.display()

# COMMAND ----------

df_joined.count()

# COMMAND ----------

df_joined.columns


# COMMAND ----------

df_joined_clean= df_joined.select(col("PO_NUMBER"), col("COMP_ID "), col("GRN Date"), col("Currency"), col("CHARG"), col("LFBNR"), col("GRN Number"), col("Year"), col("Quantity7"), col("Material Name"), col("Material Number"), col("Purchase Group"), col("Plant"), col("MENGE1"),col("LFBNR1"),col("LFPOS"),col("STATUS"), col("Quantity16"), col("Vendor Code"), col("vendor Company name"), col("Country"), col("PO Line number"), col("DATE_CREATED"), col("VENDOR_CODE"), col("PO_LINE_ITEM"), col("PO_RELEASE_DATE"), col("MAT_TYPE"), col("ORDER_QTY"), col("PENDING_QTY"), col("DELIVERY_COMPLETED"), col("RECEIVED_QTY"))

df_joined_clean.display()

# COMMAND ----------

# DBTITLE 1,otif
df_joined_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OTIF CALCULATION

# COMMAND ----------

# Add OTIF_RECEIVED Column
df_otif = df_joined_clean.withColumn("OTIF_RECEIVED", when(col("PENDING_QTY") > col("ORDER_QTY"), col("ORDER_QTY") - col("PENDING_QTY")).otherwise(col("ORDER_QTY")))
df_otif.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OTIF CALCULATION

# COMMAND ----------

df_joined_clean = df_joined_clean.withColumn("PO_RELEASE_DATE", col("PO_RELEASE_DATE").cast("date"))
df_joined_clean = df_joined_clean.withColumn("GRN Date", col("GRN Date").cast("date"))



# COMMAND ----------

first_grn_df = df_joined_clean.groupBy("PO_NUMBER").agg(min("GRN Date").alias("FirstGRNDate"))

# COMMAND ----------

df_joined_clean = df_joined_clean.alias("df").join(first_grn_df.alias("fgd"), on="PO_NUMBER", how="left")

# COMMAND ----------

df_joined_clean = df_joined_clean.withColumn(
    "Adjusted_PO_Release_Date",
    when(col("df.PO_RELEASE_DATE").isNull(), col("fgd.FirstGRNDate") - 2)
    .otherwise(col("df.PO_RELEASE_DATE"))
)

# COMMAND ----------

df_joined_clean.count()

# COMMAND ----------

df_joined_clean.display()

# COMMAND ----------

df_joined_clean = df_joined_clean.withColumn("PO_Late_Flag", when(col("Adjusted_PO_Release_Date") < col("df.GRN Date"), 1).otherwise(0))

# Compute Full_Delivery_Flag
df_joined_clean = df_joined_clean.withColumn("Full_Delivery_Flag", when(col("df.ORDER_QTY") == col("df.RECEIVED_QTY"), 1).otherwise(0))

# Compute Pending_Flag
df_joined_clean = df_joined_clean.withColumn(
    "Pending_Flag",
    when((col("df.ORDER_QTY") != col("df.RECEIVED_QTY")) & (col("df.PENDING_QTY") > 0), 1).otherwise(0)
)

# Compute OTIF_Flag
df_joined_clean = df_joined_clean.withColumn(
    "OTIF_Flag",
    when(col("PO_Late_Flag") == 1,
         when(col("Full_Delivery_Flag") == 1, 1)
         .otherwise(when(col("Pending_Flag") == 1, 0).otherwise(1)))
    .otherwise(0)
)

# COMMAND ----------

# DBTITLE 1,OTIF_PERCENTAGE_PER_PO

otif_percentage_df = df_joined_clean.groupBy("PO_NUMBER").agg(
    (spark_sum("OTIF_Flag") / count("PO_NUMBER") * 100).alias("`q`.`OTIF_Percentage_Per_PO")
)


df_joined_clean = df_joined_clean.join(otif_percentage_df, on="PO_NUMBER", how="left")
df_joined_clean.display()


display(df_joined_clean)

# COMMAND ----------

df_joined_clean.count()

# COMMAND ----------

schema = StructType([
    StructField("REQ_ID", IntegerType(), True),
    StructField("U_ID", IntegerType(), True),
    StructField("REQ_TITLE", StringType(), True),
    StructField("REQ_POSTED_ON", DateType(), True),
    StructField("Status", StringType(), True),  # Renamed "CLOSED" to "Status"
    StructField("START_TIME", DateType(), True),
    StructField("END_TIME", DateType(), True),
    StructField("DATE_CREATED", DateType(), True),
    StructField("SAVINGS", FloatType(), True),
    StructField("QUOTATION_FREEZ_TIME", DateType(), True),
    StructField("BIDDING_TYPE", StringType(), True),
    StructField("REQ_STATUS", StringType(), True)
])

# COMMAND ----------

df_analytics=spark.read.csv("/FileStore/tables/req_data_fro_analytics.csv", schema=schema, header=True)

# COMMAND ----------

df_analytics.display()

# COMMAND ----------

df_analytics.count()

# COMMAND ----------

schema_1 = StructType([
    StructField("REQ_ID", StringType(), True),
    StructField("U_ID", StringType(), True),
    StructField("ITEM_ID", StringType(), True),
    StructField("PRODUCT_NAME", StringType(), True),
    StructField("PRICE", DoubleType(), True),
    StructField("REVISED_PRICE", DoubleType(), True),
    StructField("UNIT_PRICE", DoubleType(), True),
    StructField("REV_UNIT_PRICE", DoubleType(), True),
    StructField("BID_TIME", DateType(), True),
    StructField("TYPE", StringType(), True)
])

# COMMAND ----------

schema_2 = StructType([
    StructField("NEG_ID", StringType(), True),
    StructField("U_ID", StringType(), True),
    StructField("REQ_ID", StringType(), True),  # Assuming case consistency
    StructField("Price", DoubleType(), True),
    StructField("Bid Time", DateType(), True),
    StructField("Reject_Reason", StringType(), True),
    StructField("Surrogate_id", StringType(), True),
    StructField("Surrogate_comments", StringType(), True)
])

# COMMAND ----------

df_negotiation=spark.read.csv("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/negotiation_Audit___analytics_neuland.csv", schema=schema_1, header=True)
df_negotiation.display()

# COMMAND ----------

df_item_aduit=spark.read.csv("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/Item_Audit___Analytics.csv", schema=schema_2, header=True)
df_item_aduit.display()

# COMMAND ----------



# COMMAND ----------

df_negotiation.display()

# COMMAND ----------

df_item_aduit.display()

# COMMAND ----------

inovice_schema = StructType([
    StructField("po_number", StringType(), True),
    StructField("vendor_code", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("INVOICE_NUMBER", StringType(), True),
    StructField("INVOICE_AMOUNT", DoubleType(), True),
    StructField("STATUS", StringType(), True),
    StructField("INVOICE_ID", StringType(), True),
    StructField("INVOICE_DATE", DateType(), True),  # DateType for date fields
    StructField("PO_LINE_ITEM", IntegerType(), True),
    StructField("INVOICE_QTY", IntegerType(), True),
    StructField("INVOICE_SAP_NUMBER", StringType(), True),
    StructField("INVOICE_SAP_LINE_ITEM", IntegerType(), True),
    StructField("GRN_NUMBER", StringType(), True),
    StructField("GRN_LINE_ITEM", IntegerType(), True),
    StructField("INVOICE_REJECTED_BY", StringType(), True),
    StructField("INVOICE_REJECTED_COMMENT", StringType(), True),
    StructField("INVOICE_FISCAL_YEAR", IntegerType(), True)
])

# COMMAND ----------

df_item_invoice=spark.read.csv("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/invoicedetails_analytics.csv", schema=inovice_schema, header=True)
df_item_invoice.display()

# COMMAND ----------

workflow_schema= schema = StructType([
    StructField("WF_ID", StringType(), True),
    StructField("WF_TITLE", StringType(), True),
    StructField("WF_DETAILS", StringType(), True),
    StructField("WF_MODULE", StringType(), True),
    StructField("DEPT_ID", StringType(), True)
])

# COMMAND ----------

df_workflow=spark.read.csv("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/workflows_analytics.csv", schema=workflow_schema, header=True)
df_workflow.display()

# COMMAND ----------



# COMMAND ----------

workflow_schema_2 = StructType([
    StructField("WF_STAGE_ID", StringType(), True),
    StructField("WF_ID", StringType(), True),
    StructField("WF_APPROVER", StringType(), True),
    StructField("WF_ORDER", IntegerType(), True),  # Assuming order is a numeric sequence
    StructField("WF_DEPT_ID", StringType(), True),
    StructField("WF_DESIG_ID", StringType(), True),
    StructField("WF_APPR_RANGE", DoubleType(), True)  # Assuming approval range is a numeric value
])

# COMMAND ----------

df_workflow_2=spark.read.csv("dbfs:/FileStore/shared_uploads/mvsbhargavreddy@gmail.com/Workflowtrack_analytics2.csv", schema=workflow_schema_2, header=True)
df_workflow_2.display()

# COMMAND ----------


