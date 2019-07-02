package com.aibee.bdp

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import play.api.libs.json._

import scala.collection.mutable

case class Metric(timestamp: String, value: String, name: String, labels: Map[String, String])
case class MetricWithDt(timestamp: String, value: String, name: String, labels: Map[String, String], dt: String)

/**
 * Hello world!
 *
 */
object App extends App with Logging {
  val arg = new Argument(args)
  val spark = SparkSession
    .builder
    .appName(arg.appName.apply())
    .master("local")
    .config("spark.sql.avro.compression.codec", "snappy")
//    .config("spark.sql.hive.metastore.version", "2.1.1")
//    .enableHiveSupport
    .getOrCreate
  val (startingOffset, endingOffset) = readOffsets()

  val df = spark.read.format("kafka")
    .option("kafka.bootstrap.servers", arg.kafkaBootstrapServers.apply())
    .option("subscribe", arg.topics.apply())
    .option("startingOffsets", startingOffset)
    .option("endingOffsets", endingOffset)
    .load
  writeEndingOffset()
  writeKafkaRecord()
//  spark.sql("MSCK TABLE test.test_avro")

  def readOffsets(): (String, String) = {
    var start = "earliest"
    var end = "latest"
    val minMaps = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    val maxMaps = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    if (arg.lastOffsetPath.isDefined) {
      val schema = StructType(List(
        StructField("topic", StringType),
        StructField("partition", StringType),
        StructField("minOffset", LongType),
        StructField("maxOffset", LongType),
      ))
      val df = spark.read.schema(schema).csv(arg.lastOffsetPath.apply())
      df.collect.foreach(row => {
        val topic = row.getString(0)
        val partition = row.getString(1)
        val minOffset = row.getLong(2)
        val maxOffset = row.getLong(3)
        if (arg.backfill.apply()) {
          minMaps.getOrElseUpdate(topic, mutable.HashMap[String, Long]())(partition) = minOffset
          maxMaps.getOrElseUpdate(topic, mutable.HashMap[String, Long]())(partition) = maxOffset
          start = Json.toJson(minMaps).toString
          end = Json.toJson(maxMaps).toString
        } else {
          maxMaps.getOrElseUpdate(topic, mutable.HashMap[String, Long]())(partition) = maxOffset + 1
          start = Json.toJson(maxMaps).toString
        }
      })
    }
    logInfo(f"consumer starting offsets: $start, ending offset: $end")
    (start, end)
  }

  def writeEndingOffset(): Unit = {
    df.createTempView("metrics")
    val aggr = spark.sql("select topic, partition, min(offset) as min_offset, max(offset) as offset " +
      "from metrics group by topic, partition").repartition(1)
    var filename = arg.offsetPath.apply
    filename = filename + (if (filename.endsWith("/")) "dt=" + arg.getDt else "/dt=" + arg.getDt)
    aggr.write.mode(SaveMode.Overwrite).csv(filename)
  }

  def writeKafkaRecord(): Unit = {
    import spark.implicits._
    val metricDs: Dataset[MetricWithDt] = df.select("value").as[Array[Byte]].map(row => {
      implicit val metricReads: Reads[Metric] = Json.reads[Metric]
      val t = Json.parse(row)
      Json.fromJson(t) match {
        case JsSuccess(m: Metric, _) => {
          val mutableMap = mutable.Map(m.labels.toSeq: _*)
          if (mutableMap.contains("__name__")) {
            mutableMap.remove("__name__")
          }
          MetricWithDt(m.timestamp, m.value, m.name, mutableMap.toMap,
            m.timestamp.substring(0, 10).replace("-", ""))
        }
        case _ => null
      }
    })
    val output: Dataset[MetricWithDt] = metricDs.filter(_ != null)

    // 将结果写入到hdfs
    val dir = arg.outputDir.apply()
    output.write
      .mode(SaveMode.Append)
      .format("avro")
      .partitionBy("dt")
      .save(dir)
  }
}
