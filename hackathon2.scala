package org.inceptez.spark.hack2

import org.inceptez.hack.allmethods;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;
//Learning - Since there are no Scala variable to handle Date
// importing java class to handle date
// or date can be handled as string
import java.sql.Date
//For persist Storage
import org.apache.spark.storage._

object hackathon {
  
  case class insureclass(IssuerId:String,IssuerId2:String,BusinessDate:String,StateCode:String,SourceName:String,
      NetworkName:String,NetwrokURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)
      
  def main(args:Array[String])
  {
    //val sconf = new SparkConf().setAppName("First Hackathon").setMaster("local[*]");
    //val sc = new SparkContext(sconf);
    //val sqlc = new SQLContext(sc);
    
    //sc.setLogLevel("ERROR");
    
    val spark=SparkSession.builder().appName("Sample sql app").master("local[*]")
              .config("hive.metastore.uris","thrift://localhost:9083")
              .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
              .enableHiveSupport().getOrCreate();
    
    spark.sparkContext.setLogLevel("error")

    val insinfo1csv = spark.sparkContext.textFile("hdfs://Localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv");
    
    val insinfo1head = insinfo1csv.first();
    
    val insinfo1woh = insinfo1csv.filter(x => x != insinfo1head);
    
    //Learning Take works on RDD[String]
    println("print few rows for insurance 1 file");
    insinfo1woh.take(10).foreach(println);
    
    val insinfo1 = insinfo1woh.map(x => x.split(",",-1)).filter(x => x.length == 10)
    
    val insinfo1cnt = insinfo1.count();
    val insinfo1csvcnt = insinfo1csv.count();
    
    println(s"rows removed in clean up of insurance 1 file " +(insinfo1csvcnt - insinfo1cnt));
    
    val insinfof11 = insinfo1.map(x => insureclass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),
          x(7),x(8),x(9)));
    
    //Learning - Subtract takes only RDD[String]
    // mkString is used to convert Array[String] into RDD [String]
    // fulldata.subtract(filtered data)
    val rejectdata1 = insinfo1csv.subtract(insinfo1.map(x => x.mkString(",")));
    rejectdata1.collect();
    
    val insinfo2csv = spark.sparkContext.textFile("hdfs://Localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv");
    
    val insinfo2head = insinfo2csv.first();
    
    val insinfo2woh = insinfo2csv.filter(x => x != insinfo1head);
    
    //Learning Take works on RDD[String]
    // an count must be passed to it
    // Collect is used to print all the contents of the RDD
    println("print few rows for insurance 2 file");
    insinfo2woh.take(10).foreach(println);
    
    val insinfo2 = insinfo2woh.map(x => x.split(",",-1)).filter(x => x.length == 10)
    
    val insinfo2cnt = insinfo2.count();
    val insinfo2csvcnt = insinfo2csv.count();
    
    println(s"rows removed in clean up of insurance 2 file " +(insinfo2csvcnt - insinfo2cnt));
    
    val insinfof22 = insinfo2.map(x => insureclass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),
          x(7),x(8),x(9)));
    
    //Learning - Subtract takes only RDD[String]
    // mkString is used to convert Array[String] into RDD [String]
    // fulldata.subtract(filtered data)
    val rejectdata2 = insinfo2csv.subtract(insinfo2.map(x => x.mkString(",")));
    rejectdata2.collect();
    
    //Step 12
    val inscom = insinfof11.union(insinfof22);
    
    inscom.persist(StorageLevel.MEMORY_ONLY_2);
    
    if  (inscom.count() == (insinfo2.count() + insinfo1.count()))
      println("count is matching");
    
    //Step 15
    val insdis = inscom.distinct();
   
    println("duplicate rows " +(inscom.count() - insdis.count()));
    
    //Step 16
    val insuredatarepart = inscom.repartition(8);
    
    //Step 17
    val rdd20191001 = inscom.filter(x => x.BusinessDate == "2019-10-01");
    val rdd20191002 = inscom.filter(x => x.BusinessDate == "2019-10-02");
    
    //Step 18
    //Learning to save a rdd to a hdfs location
    //rejectdata1.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rejecctdata1.txt");
    //rejectdata2.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rejecctdata2.txt");
    //inscom.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/merged_data.txt");
    //rdd20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rdd20191001.txt");
    //rdd20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rdd20191002.txt");
    
    //Step 19
    import spark.sqlContext.implicits._;
    
    val insuredaterepartdf = insuredatarepart.toDF();
    
    //Step 20
    import org.apache.spark.sql.types._

    val StructSchema = StructType(List(StructField("IssuerId1", IntegerType, true),
        StructField("IssuerId2", IntegerType, true),
        StructField("BusinessDate", DateType, true),
        StructField("StateCode", StringType, true),
        StructField("SourceName", StringType, true),
        StructField("NetworkName", StringType, true),
        StructField("NetworkURL", StringType, true),
        StructField("custnum", StringType, true),
        StructField("MarketCoverage", StringType, true),
        StructField("DentalOnlyPlan", StringType, true)));
    
    //Step 19
    val insuredaterepartdf1 = spark.sqlContext.createDataFrame(insuredaterepartdf.rdd,StructSchema);
        
    //Step 21
    val inscsvdf1 = spark.read.option("Escape",",").schema(StructSchema).option("header","true").
        csv("hdfs://Localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv");
    
    val inscsvdf2 = spark.read.option("Escape",",").schema(StructSchema).option("header","true").
        csv("hdfs://Localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv");
    
    //Step 22
    import java.time.LocalDateTime;
    import java.time.format.DateTimeFormatter;
    import org.apache.spark.sql.functions._;
    val inscsvdf11 = inscsvdf1.withColumnRenamed("StateCode","stcd").
                    withColumnRenamed("SourceName","srcnm").
                    withColumn("issueridcomposite",concat($"IssuerId1".cast(StringType),lit(" "),
                        $"IssuerId2".cast(StringType))).
                    withColumn("sysdt",lit(current_date())).
                    withColumn("systs",lit(current_timestamp())).                        
                    drop("DentalOnlyPlan");
    val inscsvdf22 = inscsvdf2.withColumnRenamed("StateCode","stcd").
                    withColumnRenamed("SourceName","srcnm").
                    withColumn("issueridcomposite",concat($"IssuerId1".cast(StringType),lit(" "),
                        $"IssuerId2".cast(StringType))).
                    withColumn("sysdt",lit(current_date())).
                    withColumn("systs",lit(current_timestamp())).                        
                    drop("DentalOnlyPlan");
    
    //Step 23
    val inscsvdf12 = inscsvdf11.na.drop();
    val inscsvdf23 = inscsvdf22.na.drop();
    println("number of rows which contains all the values " + inscsvdf12.count());
    println("number of rows which contains all the values " + inscsvdf22.count());

    
    //Step 25
    val obj = new allmethods;
    val dsludf = udf(obj.remspecialchar _);
    
    //Step 26xs
    val inscsvdf13 = inscsvdf12.withColumn("Network_Name",dsludf($"NetworkName")).drop($"NetworkName");
    val inscsvdf24 = inscsvdf23.withColumn("Network_Name",dsludf($"NetworkName")).drop($"NetworkName");
    
    //Step 27
    inscsvdf13.coalesce(1).write.mode("overwrite").json("hdfs://Localhost:54310/user/hduser/sparkhack2/inscsvdf13.json");
    
    //Step 28
    inscsvdf24.coalesce(1).write.mode("overwrite").option("sep","~").option("header","true").
                    csv("hdfs://localhost:54310/user/hduser/sparkhack2/inscsvdf24.csv");
    
    //Step 29
    inscsvdf13.write.mode("overwrite").saveAsTable("default.inscsvdf13");
    inscsvdf24.write.mode("overwrite").saveAsTable("default.inscsvdf24");
    
    //Step 30
    val custstates = spark.sparkContext.textFile("hdfs://Localhost:54310/user/hduser/sparkhack2/custs_states.csv");
    
    //Step 31
    val custfilter = custstates.map(x => x.split(",")).filter(x => x.length == 5);
    val statesfilter = custstates.map(x => x.split(",")).filter(x => x.length == 2);
    
    //Step 32
    val custstatesdf = spark.read.csv("hdfs://Localhost:54310/user/hduser/sparkhack2/custs_states.csv");
    
    //Step 33
    val custfilterdf = custstatesdf.select("_c0","_c1","_c2","_c3","_c4").filter("_c2 is not null");
    val statesfilterdf = custstatesdf.select("_c0","_c1").filter("_c2 is null");
    
    //Step 34
    custfilterdf.createOrReplaceTempView("custview");
    statesfilterdf.createOrReplaceTempView("statesview");
    
    //Step 34
    val insureviewsrc = inscsvdf11.union(inscsvdf22);
    insureviewsrc.createOrReplaceTempView("insureview");
    
    //Step 36
    //Learning udf registration for df and sql is different
    spark.udf.register("remspecialcharudf",obj.remspecialchar _);
    
    //Step 37
    val findf = spark.sql("""select remspecialcharudf(NetworkName) as cleannetworkname, CURRENT_DATE() as curdt,
                 CURRENT_TIMESTAMP() as curts, YEAR(BusinessDate) as yr, MONTH(BusinessDate) as mth,
                 case 
                 when SUBSTRING(NetworkURL,1,5) == "https" 
                 then SUBSTRING(NetworkURL,1,5)
                 when SUBSTRING(NetworkURL,1,4) == "http"
                 then SUBSTRING(NetworkURL,1,4)
                 else "noprotocol" 
                 end
                 as protocol,
                 IssuerId1, IssuerId2, BusinessDate, stcd, srcnm, NetworkName, NetworkURL,
                 custnum, MarketCoverage, issueridcomposite, sysdt, systs, statesview._c1 as statedesc, 
                 custview._c3 as age, custview._c4 as profession
                 from insureview inner join statesview
                 on statesview._c0 == insureview.stcd
                 inner join custview
                 on custview._c0 == insureview.custnum""");
    
    //Step 38
    findf.coalesce(1).write.mode("overwrite").parquet("hdfs://Localhost:54310/user/hduser/sparkhac2/finaldf.parquet");
    
    //Step 39
    findf.createOrReplaceTempView("findfview");
    
    val findf1 = spark.sql("""select AVG(age), count(statedesc)
                from findfview
                group by statedesc, protocol""");
    
    //Step 40
    val prop=new java.util.Properties();
    prop.put("user", "root")
    prop.put("password", "root")

    findf1.write.mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","findf1spark",prop)

    println("end.....");
    
  }
  
  //Step 24
  /*def remspecialchar(str:String):String=
  {
    return str.replaceAll("[0-9-?,//_()/[/]]","");
  }*/
  
}