# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 14:17:06 2022

@author: suraj
"""
import pyspark	

from pyspark.sql import SparkSession, Row	

from pyspark.sql.types import MapType, StringType	

from pyspark.sql.functions import col	

from pyspark.sql.types import StructType,StructField, StringType

# Implementing the map() transformation in Databricks in PySpark	

spark = SparkSession.builder.appName("map()_PySpark").getOrCreate()	

Sample_data = ["Project","Narmada","Gandhi","Adventures",	

"in","Gujarat","Project","Narmada","Adventures",	

"in","Gujarat","Project","Narmada"]	


Rdd = spark.sparkContext.parallelize(Sample_data)	


# Using map() transformation	

Rdd2 = Rdd.map(lambda x: (x,1))	

for element in Rdd2.collect():	

    print(element)
