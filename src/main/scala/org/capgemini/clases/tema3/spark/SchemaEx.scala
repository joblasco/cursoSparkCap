package org.capgemini.clases.tema3.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row}
import org.capgemini.curso.spark.utils.SparkSessionTrait

object SchemaEx extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {

    val inferSchemaVal = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/baby-names.csv")

    inferSchemaVal.printSchema()

    val schemaOne: StructType = StructType(
      List(
        StructField("year", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("percent", DoubleType, nullable = true),
        StructField("sex", StringType, nullable = true)
      )
    )

    val dataOne: Seq[Row] = Seq(
      Row("1900", "Paco", 0.0100, "boy"),
      Row("1901", "Maria", 0.0100, "girl"),
      Row("1902", "Juan", 0.0100, "boy")
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(dataOne), schemaOne
    )
    df.printSchema()

    df.show()

  }

}
