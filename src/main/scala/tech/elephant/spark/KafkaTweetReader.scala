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

/**
  * In Spark, reads Avro-encoded tweets from Kafka that were produced by the kafka-esque project, using the Confluent
  * Schema Registry. Doesn't do anything interesting with the message - just dumps to log.
  */
object KafkaTweetReader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Avro msgs from Kafka")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "blackcube.home:9092",
      "group.id" -> "test",
      "schema.registry.url" -> "http://confluent.home:8081")
    val topicSet = Set("tweets")

    val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet)

    // Just dump a message to prove we can read it
    val lines = messages.map(msg => {
      val (k, v) = msg
      v.toString()
    })
    lines.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
