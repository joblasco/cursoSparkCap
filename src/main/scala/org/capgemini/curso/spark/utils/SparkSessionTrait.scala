package org.capgemini.curso.spark.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("Spark Cap Curse")
    .getOrCreate()

}
