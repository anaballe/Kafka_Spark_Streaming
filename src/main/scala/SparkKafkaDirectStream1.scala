package spark.kafka

import _root_.kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
object SparkKafkaDirectStream {
   def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: SparkKafkaDirectStream <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 10 second batch interval
    val sparkConf = new SparkConf()
                        .setAppName("SparkKafkaDirectStream")
                        .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10)) 

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicSet = topics.split(",").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,  
    StringDecoder]( ssc, kafkaParams, topicSet)

    // Get the lines, print them
    val lines = kafkaStream.map(_._2)
    lines.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  } //end of main
} //end of SparkKafkaDirectStream
