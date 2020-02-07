package br.com.smtx

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object nasa_challenge {

  def replaceBracket(file:String):String = {
      try {
          return file.replace("[", "")
      } catch {
          case e: Exception => file    
      }
  }
  def formatData(file:String):Seq[String] = {
      try {
          val arr = file.split(" ")
          val tam = arr.size-1
          return Seq(arr(0), arr(1), arr(2), arr(3), arr(4), arr.slice(5, tam-2).mkString(" "), arr(tam-1), arr(tam))
      } catch {
          case e: Exception => Seq("","","","","","","","")
      }
  }

  def cleanFile(spark:SparkSession, file:String):DataFrame = {
      val bracket_udf = udf((timestamp:String) => replaceBracket(timestamp))
      val func_udf = udf((bad_records:String) => formatData(bad_records))
      val schema_untyped = new StructType().
                  add("address", "string").
                  add("lixo1", "string").add("lixo2", "string").
                  add("timestamp", "string").add("timezone", "string").
                  add("request", "string").add("return_code", "int").
                  add("returned_bytes", "string").
                  add("_corrupt_record", "string")
      val df_nasaJuly = spark.read.format("csv").
                  option("delimiter", " ").
                  schema(schema_untyped).
                  option("mode", "PERMISSIVE").
                  load(s"${file}")
      val good = df_nasaJuly.filter("_corrupt_record is null and return_code is not null")
      val bad = df_nasaJuly.filter("_corrupt_record is not null")
      val bad_records = bad.cache().withColumn("corrupt_record", col("_corrupt_record")).select("corrupt_record")
      val good_records = good.select("address", "lixo1", "lixo2", "timestamp", "timezone", "request", "return_code", "returned_bytes")
      val bad_records_corrected = bad_records.select(func_udf(col("corrupt_record"))(0).alias("address"), func_udf(col("corrupt_record"))(1).alias("lixo1"), func_udf(col("corrupt_record"))(2).alias("lixo2"), func_udf(col("corrupt_record"))(3).alias("timestamp"), func_udf(col("corrupt_record"))(4).alias("timezone"), func_udf(col("corrupt_record"))(5).alias("request"), func_udf(col("corrupt_record"))(6).alias("return_code"), func_udf(col("corrupt_record"))(7).alias("returned_bytes"))
      val complete_records = good_records.union(bad_records_corrected)
      val complete_records_updated = complete_records.withColumn("timestamp", bracket_udf(col("timestamp")))
      return complete_records_updated
  }

  def question1(spark:SparkSession, df:DataFrame, answer_path:String) = {
      import spark.implicits._
      val total_hosts = df.select(countDistinct("address").alias("total_hosts"))
      total_hosts.show(false)
      total_hosts.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(s"${answer_path}")
  }

  def question2(spark:SparkSession, df:DataFrame, answer_path:String) = {
      import spark.implicits._
      val count_return_codes = df.groupBy($"return_code").agg(count($"return_code").as("count"))
      count_return_codes.show(false)
      count_return_codes.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(s"${answer_path}")
  }

  def question3(spark:SparkSession, df:DataFrame, answer_path:String) = {
      import spark.implicits._
      val address_404_error = df.groupBy("address","return_code").count().where("return_code = 404")
      val address_404_error_desc = address_404_error.orderBy($"count".desc)
      address_404_error_desc.show(5, false)
      address_404_error_desc.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(s"${answer_path}")
  }

  def question4(spark:SparkSession, df:DataFrame, answer_path:String) = {
      import spark.implicits._
      val dataOut = df.withColumn("timestamp2", unix_timestamp($"timestamp", "dd/MMM/yyyy:H:m:s").cast(TimestampType))
      val records_updated = dataOut.withColumn("date", to_date($"timestamp2"))
      val records_updated_final = records_updated.groupBy("date","return_code").count().where("return_code = 404")
      records_updated_final.show(100, false)
      records_updated_final.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(s"${answer_path}")
  }

  def question5(spark:SparkSession, df:DataFrame, answer_path:String) = {
      import spark.implicits._
      val sum_bytes =  df.agg(sum("returned_bytes"))
      sum_bytes.show(false)
      sum_bytes.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(s"${answer_path}")
  }

  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      
      val home_path = args(0)
      val july_file = s"${home_path}/JUL"
      val aug_file  = s"${home_path}/AUG"

      val df_july = cleanFile(spark, july_file).cache()
      val df_aug  = cleanFile(spark, aug_file).cache()
      val df_july_aug = df_july.union(df_aug)
      
      println("Questão 1")
      question1(spark, df_july, s"${home_path}/answer_1/JUL")
      question1(spark, df_aug, s"${home_path}/answer_1/AUG")
      question1(spark, df_july_aug, s"${home_path}/answer_1/JUL_AUG")

      println("Questão 2")
      question2(spark, df_july, s"${home_path}/answer_2/JUL")
      question2(spark, df_aug, s"${home_path}/answer_2/AUG")
      question2(spark, df_july_aug, s"${home_path}/answer_2/JUL_AUG")

      println("Questão 3") 
      question3(spark, df_july, s"${home_path}/answer_3/JUL")
      question3(spark, df_aug, s"${home_path}/answer_3/AUG")
      question3(spark, df_july_aug, s"${home_path}/answer_3/JUL_AUG")

      println("Questão 4")
      question4(spark, df_july, s"${home_path}/answer_4/JUL")
      question4(spark, df_aug, s"${home_path}/answer_4/AUG")
      question4(spark, df_july_aug, s"${home_path}/answer_4/JUL_AUG")

      println("Questão 5")
      question5(spark, df_july, s"${home_path}/answer_5/JUL")
      question5(spark, df_aug, s"${home_path}/answer_5/AUG")
      question5(spark, df_july_aug, s"${home_path}/answer_5/JUL_AUG")
  }
}
