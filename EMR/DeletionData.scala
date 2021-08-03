/**
 * This program aims to carry out
 * the deletion of sensitive data as request
 */

// Imports Apache Spark
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

// Import Spark Daria
import com.github.mrpowers.spark.daria.sql.DariaWriters

// Imports Apache Hudi
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.keygen.ComplexKeyGenerator


object Deletion extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def startSparkSession(masternode: String = "local"): SparkSession = {
    /**
     * This function initialize the working session
     * on the spark cluster
     */

    val name_session = SparkSession.builder()
      .master(masternode)
      .appName("DeletionData")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    name_session
  }

  def check_delete(ssn_check: String): String = {
    if (ssn_check == null) {
      val token: String = "SUCESSO"
      token
    } else if (ssn_check == "null"){
      val token: String = "SUCESSO"
      token
    } else {
      val token: String = "FALHA"
      token
    }
  }

  def change_value_null(value: String): String = {
    val value_change: String = "null"
    value_change
  }
  def change_value_integer(value: Integer): Integer = {
    val value_change: Integer = 0
    value_change
  }

  def change_value_status_null(value: String): String = {
    if (value == null){
      val value_change: String = "FALHA"
      value_change
    } else if (value == "null"){
      val value_change: String = "FALHA"
      value_change
    }
    else {
      value
    }
  }

  def createUDF_check_delete(): UserDefinedFunction = {
    /**
     * This function creates a UDF role
     */
    val function_udf = udf[String, String](check_delete)
    function_udf
  }

  def createUDF_change(): UserDefinedFunction = {
    /**
     * This function creates a UDF role
     */
    val function_udf = udf[String, String](change_value_null)
    function_udf
  }

  def createUDF_change_integer(): UserDefinedFunction = {
    /**
     * This function creates a UDF role
     */
    val function_udf = udf[Integer, Integer](change_value_integer)
    function_udf
  }

  def createUDF_change_status_null(): UserDefinedFunction = {
    /**
     * This function creates a UDF role
     */
    val function_udf = udf[String, String](change_value_status_null)
    function_udf
  }

  def main(args: Array[String]): Unit = {
    /**
     * Performs all necessary steps for deletion data.
     */
    // Args
    val input_path_json: String = args(0) + "deletiondata.json"
    val input_path_data_pii: String = args(1)
    val output_path_json: String = args(2) + "deletiondatanew.json"
    val output_path_json_temp: String = args(2) + "/temp"
    val node: String = args(3)
    try{
      //Spark Session
      val spark = startSparkSession(node)
      // Spark Context
      val spark_context = spark.sparkContext
      //List Columns With Join
      val columns_clean_join = Seq("token_id", "ssn", "rg", "name", "fancy_name", "contact_name", "gender", "birthday", "inscricao_municipal", "address", "number", "complement", "zip_code", "ibge_code", "Hash")
      // Remove columns Hudi
      val columns_clean_hudi = Seq("token_id", "ssn", "rg", "name", "fancy_name", "contact_name", "gender", "birthday",
        "inscricao_municipal", "address", "number", "complement", "zip_code", "ibge_code")
      // Read JSON
      val df_json = spark.read.format("json").load(input_path_json)
      print("\nRead JSON\n")
      // Read Bucket PII-DATA
      val df_data_pii = spark.read.format("hudi").load(input_path_data_pii + "*")
      print("\nRead Bucket PII")
      // Change Column ssn to JSON_SSN
      val df_json_clean = df_json.select(col("ssn") as "json_ssn", col("Hash"))
      print("\nChange Column ssn to JSON_SSN")
      // Join between JSON and DATA after select columns in list COLUMNS_CLEAN_JOIN
      val df_join = df_data_pii.join(df_json_clean, df_data_pii("ssn") === df_json_clean("json_ssn"), "inner")
        .select(columns_clean_join.map(m => col(m)): _*)
      print("\nJoin between JSON and DATA after select columns in list COLUMNS_CLEAN_JOIN")
      // Select Columns in List COLUMNS_CLEAN_HUDI
      val df_data_pii_select = df_join.select(columns_clean_hudi.map(m => col(m)): _*)
      print("\nSelect Columns in List COLUMNS_CLEAN_HUDI")
      // Create UDF change value to null
      val change_value = createUDF_change()
      val change_value_integer = createUDF_change_integer()
      // Apply UDF in DF
      val df_token = df_data_pii_select
        .withColumn("ssn", change_value(col("ssn")))
        .withColumn("rg", change_value(col("rg")))
        .withColumn("name", change_value(col("name")))
        .withColumn("fancy_name", change_value(col("fancy_name")))
        .withColumn("contact_name", change_value(col("contact_name")))
        .withColumn("gender", change_value(col("gender")))
        .withColumn("inscricao_municipal", change_value(col("inscricao_municipal")))
        .withColumn("address", change_value(col("address")))
        .withColumn("number", change_value(col("number")))
        .withColumn("complement", change_value(col("complement")))
      print("\nApply UDF in DF")
      // Write DF in bucket
      df_token.write.format("hudi")
        .options(getQuickstartWriteConfigs)
        .option(OPERATION_OPT_KEY,"upsert")
        .option(PRECOMBINE_FIELD_OPT_KEY, "token_id")
        .option(RECORDKEY_FIELD_OPT_KEY, "token_id")
        .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[ComplexKeyGenerator].getName)
        .option(PARTITIONPATH_FIELD_OPT_KEY, "")
        .option(TABLE_NAME, "clients")
        .mode(Append)
        .save(input_path_data_pii)
      print("\nWrite DF in Bucket")
      // Read Again Bucket PII_DATA
      val df_data_pii_new = spark.read.format("hudi").load(input_path_data_pii + "*").select("token_id", "ssn")
      print("\nRead Again Bucket PII_DATA")
      // Select Column TOKEN and HASH from DF_JOIN
      val df_join_clean_after = df_join.select(col("token_id") as "token", col("Hash"))
      print("\nSelect Column TOKEN and HASH from DF_JOIN")
      // Join between NEW DATA and DF_JOIN after select columns in list COLUMNS CLEAN_JOIN
      val df_join_after = df_data_pii_new
        .join(df_join_clean_after, df_data_pii_new("token_id") === df_join_clean_after("token"), "inner")
      print("\nJoin between NEW DATA and DF_JOIN after select columns in list COLUMNS CLEAN_JOIN")
      // Create UDF check deletion
      val status_delete = createUDF_check_delete()
      // Apply UDF
      val df_status = df_join_after.withColumn("Status", status_delete(col("ssn")))
        .select(col("Hash"), col("Status"))
      print("\nApply UDF")
      // Join JSON and STATUS
      val df_hash = df_json
        .join(df_status, df_status("Hash") === df_json("Hash"), "leftanti")
        .select(col("Hash"))
      print("\nJoin JSON and STATUS")
      // Apply UDF Change Value
      val df_status_after = df_hash.withColumn("Status", change_value(col("Hash")))
      print("\nApply UDF Change Value")
      // Create UDF Change Value Status
      val change_status_null = createUDF_change_status_null()
      print("\nCreate UDF Change Value Status")
      // Create Union DF between Status e Status Original
      val df_union_status = df_status.union(df_status_after)
      print("\nCreate Union DF between Status e Status Original")
      // Apply UDF Change Status Null
      val df_operation = df_union_status.withColumn("Status", change_status_null(col("Status")))
        .select(col("Hash"), col("Status"))
      print("\nApply UDF Change Status Null")
      // Write DF in JSON File
      DariaWriters.writeSingleFile(
        df_operation,
        "json",
        spark_context,
        output_path_json_temp,
        output_path_json,
        "overwrite")
      print("\nWrite DF in JSON File")
  } catch {
      case e: org.apache.parquet.io.ParquetDecodingException =>
        print(e)
        System.exit(1)
      case e: java.lang.UnsupportedOperationException =>
        print(e)
        System.exit(1)
      case e: org.apache.hudi.exception.HoodieException =>
        print(e)
        System.exit(1)
      case e: org.apache.hudi.exception.HoodieUpsertException =>
        print(e)
        System.exit(1)
      case e: java.util.concurrent.ExecutionException =>
        print(e)
        System.exit(1)
      case e: org.apache.spark.SparkException =>
        print(e)
        System.exit(1)
    }
  }
}