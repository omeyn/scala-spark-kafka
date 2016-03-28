package tech.elephant.spark

import grizzled.slf4j.Logger
import io.confluent.kafka.serializers.{KafkaAvroDecoder, KafkaAvroDeserializer}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/**
  * In Spark, reads Avro-encoded tweets from Kafka that were produced by the kafka-esque project, using the Confluent
  * Schema Registry. Maintains a naive count of popular hashtags and logs them every few seconds.
  *
  * NOTE: This is just me playing - there are surely better ways to do this both in terms of Scala, Spark, and Kafka!
  */
object KafkaTweetReader {

  val logger = Logger[this.type]

  case class Tweet(id: Long, username: String, status: String)
  var trending = scala.collection.mutable.Map[String, Int]()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Avro msgs from Kafka")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ssc = new StreamingContext(sc, Seconds(10))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "blackcube.home:9092",
      "group.id" -> "test",
      "schema.registry.url" -> "http://confluent.home:8081")
    val topicSet = Set("tweets")

    val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet)

    messages.foreachRDD { rdd =>
      rdd.foreach { rawRecord =>
        val record = rawRecord._2.asInstanceOf[GenericRecord]
        val tweet = Tweet(record.get("id").asInstanceOf[Long], record.get("username").toString, record.get("status").toString)
        val pattern = new Regex("\\B#\\w\\w+")
        val hashtags = pattern findAllIn tweet.status
        hashtags.foreach( ht => {
          val rawCount = trending.get(ht)
          val count = if (rawCount.isDefined) rawCount.get.toInt else 0
          if (count == 0) {
            trending.put(ht, 1)
          } else {
            trending.put(ht, count+1)
          }
        })

        val sortedTrending = ListMap(trending.toSeq.sortWith(_._2 > _._2): _*).take(10)
        logger.info("TRENDING:")
        sortedTrending.foreach(t => {
          val (ht, htcount) = t
          logger.info(ht + " " + htcount.toString)
        })
      }

      // keep the ht map from growing unbounded
      val sortedTrending = ListMap(trending.toSeq.sortWith(_._2 > _._2): _*).take(100)
      trending = scala.collection.mutable.Map(sortedTrending.toSeq: _*)
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
