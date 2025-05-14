# Databricks notebook source
# Import pyspark sql types and functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
                                StructField('Item_Identifier', StringType(), True),
                                StructField('Item_Weight', StringType(), True),
                                StructField('Item_Fat_Content', StringType(), True),
                                StructField('Item_Visibility', StringType(), True),
                                StructField('Item_Type', StringType(), True),
                                StructField('Item_MRP', StringType(), True),
                                StructField('Outlet_Identifier', StringType(), True),
                                StructField('Outlet_Establishment_Year', StringType(), True),
                                StructField('Outlet_Size', StringType(), True),
                                StructField('Outlet_Location_Type', StringType(), True),
                                StructField('Outlet_Type', StringType(), True),
                                StructField('Item_Outlet_Sales', StringType(), True)
])

# COMMAND ----------

df = spark.read.format('csv').schema(my_struct_schema).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING FUNCTIONS

# COMMAND ----------

df.withColumn('Item_Type', upper('Item_Type')).display()

# COMMAND ----------

df.withColumn('Item_Type', lower('Item_Type')).display()

# COMMAND ----------

df.withColumn('Item_Type', initcap('Item_Type')).display()

# COMMAND ----------

# Below mentioned are to just to select the columns
# convert all initials into uppercase
df.select(initcap('Item_Type').alias('Item')).display()

# COMMAND ----------

# convert to upper
df.select(upper('Item_Type').alias('ITEM')).display()

# COMMAND ----------

# convert to lower
df.select(lower('Item_Type').alias('item')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FUNCTIONS

# COMMAND ----------

# add a column with current date
df = df.withColumn('curr_date', current_date())

# COMMAND ----------

df.display()

# COMMAND ----------

# create a column by adding 7 days
df = df.withColumn('week_after', date_add('curr_date', 7))
df.display()

# COMMAND ----------

# create a column by going back 7 days
df = df.withColumn('week_before', date_add('curr_date', -7))
# we can also use df.withColumn('week_before', date_sub('curr_date', 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE DIFF

# COMMAND ----------

df = df.withColumn('date_diff', datediff('curr_date', 'week_before'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FORMAT

# COMMAND ----------

df = df.withColumn('week_after', date_format('week_after', 'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Nulls

# COMMAND ----------

# drop the rows if any of the columns has null
df.dropna('any').display()

# COMMAND ----------

# drop the rows if all of their columns has null
df.dropna('all').display()

# COMMAND ----------

# drop the rows if a specific subset of columns has null values
df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filling Nulls

# COMMAND ----------

# replace all nulls in all columns with a value
df.fillna('NotAvailable').display()

# COMMAND ----------

# replace all nulls in a subset of columns with a value
df.fillna('NotAVailable', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT and Indexing

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE
# MAGIC #### It explodes each element in list into separate row

# COMMAND ----------

df_1 = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_1.display()

# COMMAND ----------

df_1.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS

# COMMAND ----------

df_1 = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))

# COMMAND ----------

df_1.withColumn('Type1_flag', array_contains('Outlet_Type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group By

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP').alias('ItemAvg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group By - Collect List

# COMMAND ----------


book_data = [('user1', 'book1'), ('user1', 'book2'), ('user2', 'book3'), ('user3', 'book4'), ('user3', 'book5')]
book_schema = 'username string, books string'
df_books = spark.createDataFrame(book_data, book_schema)
df_books.display()

# COMMAND ----------

df_books.groupBy('username').agg(collect_list('books')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

df_for_pivot = df.select(col('Item_Type'), col('Outlet_Size'), col('Item_MRP'))
df_for_pivot.display()

# COMMAND ----------

df_for_pivot.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN-OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

df = df.withColumn('Veg_flag', when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2

# COMMAND ----------

df.withColumn('Veg_exp_flag', when((col('Veg_flag')=='Veg') & (col('Item_MRP')<100), 'Veg_Inexpensive')\
                                    .when((col('Veg_flag')=='Veg') & (col('Item_MRP')>100), 'Veg_Expensive')\
                                        .otherwise('Non-Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### INNER JOIN
# MAGIC #### rows that are common in two dataframes

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEFT JOIN
# MAGIC #### Keep all records in left table

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RIGHT JOIN
# MAGIC #### Keep all records in right table

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANTI JOIN
# MAGIC #### Keep the records only available in df1 (left table), but not in df2

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW NUMBER, RANK, DENSE RANK

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('RowNum_Col', row_number().over(Window.orderBy(col('Item_Identifier'))))\
    .withColumn('Rank_Col', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('DenseRank_Col', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### CUMULATIVE SUM

# COMMAND ----------

df.withColumn('CumSum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TOTAL SUM

# COMMAND ----------

df.withColumn('TotalSum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS (UDF)

# COMMAND ----------

def my_fun(x):
    return x*x

# COMMAND ----------

my_udf_fun = udf(my_fun)

# COMMAND ----------

df.withColumn('my_square_MRP', my_udf_fun(col('Item_MRP').cast('float'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITE

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### APPEND MODE - adds one more file if file exists

# COMMAND ----------

df.write.format('csv').mode('append').option('path', '/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OVERWRITE MODE - overwrites the folder

# COMMAND ----------

df.write.format('csv').mode('overwrite').option('path', '/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ERROR MODE - throws error if file with that name already exists

# COMMAND ----------

#df.write.format('csv').mode('error').option('path', '/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### IGNORE MODE - ignores if a file with that name already exists

# COMMAND ----------

df.write.format('csv').mode('ignore').option('path', '/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARQUET

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path', '/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TABLE

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('myTable')

# COMMAND ----------

