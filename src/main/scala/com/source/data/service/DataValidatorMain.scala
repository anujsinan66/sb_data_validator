package com.source.data.service

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.io.FileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema
import java.io.File
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.Row
import scala.util.parsing.json.JSONObject
import com.source.data.service.utils.DataValidation
import java.util.Properties
import java.io.FileInputStream
import org.apache.hadoop.fs.FileUtil
import java.net.URI
//import org.apache.hadoop.conf.Configuration

/**
 * @author Anuj Singh
 * This job read xml ,json data and perform validation on it. After performing validation we get valid and invalid record.Currently we are taking
 * valid record and perform join operation on dataframe and get desire output.
 * 
 */
  
  object DataValidatorMain {
      def main( args: Array[ String ] ): Unit = {
         //buildspark session with basic config
         val spark = SparkSession.builder.appName("data-processing") .master("local[*]") .getOrCreate
         val sparkProperties = new Properties()
         import scala.collection.JavaConverters._
         sparkProperties.load( new FileInputStream("conf/spark.properties"))
         val sparkPropertiesMap = sparkProperties.asScala.toMap
         
         //add spark properties to the spark config
         for ( (key, value) <- sparkPropertiesMap) {
                 spark.conf.set(key.trim(), value.trim())
         }
         
         // read xml data from local. we can as read it from s3 just use our setHaddopProperties method of this class
         var xmlDataDf = spark.read.format("com.databricks.spark.xml") .option("rootTag", "actitities").option("rowTag", "activity").load("input/activity.xml")
         val xmlSchemaObj = new Schema.Parser().parse(new File("conf/xmlActivityAvroSchema.avsc")  )
         var xmlSparkSchema = getSchema(xmlSchemaObj)
         
         //validate the json after converting data to json
         var  xmlValidatedArr = xmlDataDf.collect().map(row => DataValidation.validateJson(convertRowToJSON(row),xmlSchemaObj.toString()))

         val xmlValidatedRDD = spark.sparkContext.parallelize(xmlValidatedArr)
         //xmlValidatedRDD contain valid and invalid record but we are taking only valid if we want to store invalid record then we can load that data from into s3.
         var finalValidRdd = xmlValidatedRDD.filter(data => (data != null && data.isValid()))
         val finalXmlRdd = finalValidRdd.map(data => data.getJson)
         var finalXMLDf = spark.read.json(finalXmlRdd)
         finalXMLDf.show()
         
         //read json data and avero schema.
         var jsonDataDf = spark.read.json("input/activityDesc.json")
         val jsonSchemaObj = new Schema.Parser().parse(new File("conf/jsonActivityAvroSchema.avsc")  )
         var jsonSparkSchema = getSchema(jsonSchemaObj)
         
         // perform validation on json data that will give us valid and invalid records
         var  jsonValidatedArr = jsonDataDf.collect().map(row => DataValidation.validateJson(convertRowToJSON(row),jsonSchemaObj.toString()))
         jsonValidatedArr.foreach(data => println(data.getJson))
         
         //convert Arra[string] to RDD
         val jsonValidatedRDD = spark.sparkContext.parallelize(jsonValidatedArr)
         // get valid data from RDD
         var finalJsonValidRdd = jsonValidatedRDD.filter(data => (data != null && data.isValid()))
         val finalJsonRdd = finalJsonValidRdd.map(data => data.getJson)
         var finalJsonDf = spark.read.json(finalJsonRdd)
         finalJsonDf.show()
//         var jsonDf = spark.read.schema(jsonSparkSchema).json("conf/activityDesc.json")
//         var xmlDf = spark.read.schema(xmlSparkSchema).format("com.databricks.spark.xml") .option("rootTag", "actitities").option("rowTag", "activity").load("conf/activity.xml")
//       
         //json two dataframe base on common value
        val output = finalXMLDf.join(finalJsonDf,finalXMLDf("websiteName") ===  finalJsonDf("website") && finalXMLDf("userName") ===  finalJsonDf("user"),"left")
        
        //take required coloumns
        val finalDf = output.select("activityTypeCode", "activityTypeDescription")
        output.show()
        finalDf.show()
        finalDf.coalesce(1).write.mode(SaveMode.Overwrite).json("temp/finaldata")
        mergeTextFiles(spark,"temp/finaldata","output/finaldata.txt",true)
      }

      /**
       * this is very important method where we are converting different format of data into single json format
       * so that we can perform validation on json with avro schema
       * */
      def convertRowToJSON(row: Row): String = {
          val m = row.getValuesMap(row.schema.fieldNames)
          JSONObject(m).toString()
      }
      
      /**
       * this method is used to create schema from avero file
       * */
      def getSchema(xmlSchemaObj: Schema): StructType ={
        var xmlSparkSchema : StructType = new StructType
        import scala.collection.JavaConversions._     
         for(field <- xmlSchemaObj.getFields()){
          xmlSparkSchema = xmlSparkSchema.add(field.name, SchemaConverters.toSqlType(field.schema).dataType)
         }
        return xmlSparkSchema
      }
      
      /**
       * This method create a single output file. when we perform write operation in spark we get 
       * outfile in a folder with few other files like success but we want output file not a folder 
       * this method create single out file
       *  */
    def mergeTextFiles(spark : SparkSession, srcPath: String, dstPath: String, deleteSource: Boolean): Unit =  {
        val config = spark.sparkContext.hadoopConfiguration
        val fs: FileSystem = FileSystem.get(new URI(srcPath), config)
        FileUtil.copyMerge(
          fs, new Path(srcPath), fs, new Path(dstPath), deleteSource, config, null
        )
     }
    /**
     * This method we can use when we want to load and read data from s3 location
     * */
     /*def setHaddopProperties( configuration:Configuration){
          configuration.set("fs.s3a.path.style.access", "true")
          configuration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          configuration.set("fs.s3a.fast.upload", "true")  
          configuration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      }*/
   }
     