package org.capgemini.clases.tema8.spark

import org.capgemini.curso.spark.utils.SparkSessionTrait
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Udf extends App with SparkSessionTrait{

  val udfExampleDf = spark.range(5).toDF("num")
  udfExampleDf.show()

  //Pasos para crear un UDF
  // 1- Definición UDF
  def power3 (number: Double): Double = number * number * number

  // 2- Registro
  val power3Udf = udf(power3(_: Double): Double)

  //Uso
  udfExampleDf.select(power3Udf(col("num"))).show()
  // ejemplo de error
  // udfExampleDf.select(power3Udf(col("fdsafd"))).show()

  //EJEMPLO PYTHON
  /*
    udfExampleDF = spark.range(5).toDF("number")
    def power3(double_value):
      return double_value ** 3

    from pyspark.sql.functions import udf
    power3udf = udf(power3)

    from pyspark.sql.functions import col
    udfExampleDF.select (power3udf(col("number"))).show(5)
  */


  //ejemplo 2 UDF UpperCase
  val schemaOne: StructType = StructType(
    List(
      StructField("Campo1", StringType, true),
      StructField("Campo2", StringType, true)
    )
  )
  val dataOne: Seq[Row] = Seq(
    Row("valor1.1", "valor1.2"),
    Row("valor2.1", "valor2.2")
  )

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(dataOne),
    schemaOne
  )

  df.show()

  def upperDf (x: String) :String = x.toUpperCase()

  val upperUdf = udf(upperDf(_: String): String)

   df.select(upperUdf(col("Campo1")), col("Campo2")).show()

  //Función Sql UPPER
  df.select(upper(col("Campo1")), col("Campo2")).show()

  /////////////



  // Ejemplo 3 filter  UDF
  // Creación DF
  val dfFilter = spark.createDataFrame(
    Seq(
      ("valor1", "valor1"),
      ("Valor2", "Valor3")
    )).toDF("text", "text2")

  //Definción Función
  def filterUdf(r: Row) = {r.get(0) ==  r.get(1)}

  //Registrar Udf
  val myUdf = udf(filterUdf _)

  dfFilter.filter(myUdf(struct(col("text"), col("text2")))).show()

  //se añade uso con map
  df.filter(myUdf(struct(df.columns.map(df(_)): _*))).show

}
