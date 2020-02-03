import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

def cleanFile(file:String):DataFrame = {
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
                load(file)
    val good = df_nasaJuly.filter("_corrupt_record is null and return_code is not null")
    val bad = df_nasaJuly.filter("_corrupt_record is not null")
    val bad_records = bad.cache().withColumn("corrupt_record", col("_corrupt_record")).select("corrupt_record")
    val good_records = good.select("address", "lixo1", "lixo2", "timestamp", "timezone", "request", "return_code", "returned_bytes")
    val bad_records_corrected = bad_records.select(func_udf(col("corrupt_record"))(0).alias("address"), func_udf(col("corrupt_record"))(1).alias("lixo1"), func_udf(col("corrupt_record"))(2).alias("lixo2"), func_udf(col("corrupt_record"))(3).alias("timestamp"), func_udf(col("corrupt_record"))(4).alias("timezone"), func_udf(col("corrupt_record"))(5).alias("request"), func_udf(col("corrupt_record"))(6).alias("return_code"), func_udf(col("corrupt_record"))(7).alias("returned_bytes"))
    val complete_records = good_records.union(bad_records_corrected)
    val complete_records_updated = complete_records.withColumn("timestamp", bracket_udf(col("timestamp")))
    return complete_records_updated
}

def question1(df:DataFrame) = {
    val total_hosts = df.select(countDistinct("address").alias("total_hosts"))
    total_hosts.show(false)
}

def question2(df:DataFrame) = {
    val count_return_codes = df.groupBy($"return_code").agg(count($"return_code").as("count"))
    count_return_codes.show(false)
}

def question3(df:DataFrame) = {
    val address_404_error = df.groupBy("address","return_code").count().where("return_code = 404")
    val address_404_error_desc = address_404_error.orderBy($"count".desc)
    address_404_error_desc.show(5, false)
}

def question4(df:DataFrame) = {
    val dataOut = df.withColumn("timestamp2", unix_timestamp($"timestamp", "dd/MMM/yyyy:H:m:s").cast(TimestampType))
    val records_updated = dataOut.withColumn("date", to_date($"timestamp2"))
    val records_updated_final = records_updated.groupBy("date","return_code").count().where("return_code = 404")
    records_updated_final.show(100, false)
}

def question5(df:DataFrame) = {
    val sum_bytes =  df.agg(sum("returned_bytes"))
    sum_bytes.show(false)
}

def main() = {
    val july_file = "file:///home/thais/Documentos/nasa_challenge/NASA_access_log_Jul95.gz"
    val aug_file  = "file:///home/thais/Documentos/nasa_challenge/NASA_access_log_Aug95.gz"

    val df_july = cleanFile(july_file)
    val df_aug  = cleanFile(aug_file)
    val df_july_aug = df_july.union(df_aug)
    
    println("Questão 1")
    question1(df_july)
    question1(df_aug)
    question1(df_july_aug)

    println("Questão 2")
    question2(df_july)
    question2(df_aug)
    question2(df_july_aug)

    println("Questão 3") 
    question3(df_july)
    question3(df_aug)
    question3(df_july_aug)

    println("Questão 4")
    question4(df_july)
    question4(df_aug)
    question4(df_july_aug)

    println("Questão 5")
    question5(df_july)
    question5(df_aug)
    question5(df_july_aug)
}
