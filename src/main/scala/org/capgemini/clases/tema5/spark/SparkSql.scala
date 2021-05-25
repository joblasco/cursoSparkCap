package org.capgemini.clases.tema5.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkSql extends App {

  //SparkSession con EnableHiveSupport
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[1]")
    .appName("Spark Cap Course")
    .getOrCreate()

  //Creacion Vista Temporal
  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_bibliotecas_prestamos_202008.csv")
    .createOrReplaceTempView("biblio_prestamo_view")

  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_tipo_sucursal_202009.csv")
    .createOrReplaceTempView("sucursal_prestamo_view")
  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_Co_tabla_de_codigos_tipo_de_biblioteca_202009.csv")
    .createOrReplaceTempView("bibliotecas_view")
  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_de_codigos_tipo_de_lector_202009.csv")
    .createOrReplaceTempView("codigos_tipo_lector_view")
  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_de_codigos_tipo_de_ejemplar_202009.csv")
    .createOrReplaceTempView("codigos_tipo_ejemplar_view")
  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_de_codigos_soporte_202009.csv")
    .createOrReplaceTempView("codigos_soporte_view")

  //Ejemplos Sql
    spark.sql("SHOW TABLES").show()

    spark.sql("select * from biblio_prestamo_view").show(false)

    spark.sql("DESCRIBE biblio_prestamo_view").show()

    spark.sql("DROP TABLE src")

    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

    spark.sql("SHOW TABLES").show()

    spark.sql("INSERT INTO TABLE src values (1, 'UNO')")

    spark.sql("select * from src").show()

  //Ejemplo spark
  val df =  spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_tipo_sucursal_202009.csv")

  df.select(col("Biblioteca")).show()

  //Write
  val codigoSoporteDF = spark.read.option("delimiter", ";").option("header","true")
    .csv("src/main/resources/prestamo_libros/C_tabla_de_codigos_soporte_202009.csv")

  codigoSoporteDF.select(col("Codigo")).show()

  codigoSoporteDF.write.option("path", "/Users/ivaniglesias/Documents/Trabajo/Documentos/Beca/EjemplosFormacion/Tema5-SparkSql/tabla")
    .format("csv")
    .mode("append")
    .saveAsTable("pruebaSave")

 spark.sql("SHOW TABLES").show()

}
