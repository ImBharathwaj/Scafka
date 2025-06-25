package org.tamaai.com

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import java.*
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

object ConsumerWithShutdown {
  def main(args: Array[String]): Unit = {
    ReceiveRecords()
  }
  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  def ReceiveRecords(): Unit = {
    log.info("I am Kafka Consumer with Shutdown!");

    val groupId = "my-scafka-id"
    val properties: Properties = new Properties()

    //  Create producer properties
    properties.setProperty("bootstrap.servers","localhost:9092")

    // Create consumer configs
    properties.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer",classOf[StringDeserializer].getName)

    properties.setProperty("group.id", groupId)
    properties.setProperty("auto.offset.reset", "earliest")

    // Create a consumer
    val consumer = new KafkaConsumer[String, String](properties)

    //  get a reference to the main thread
    val mainThread = Thread.currentThread()

    // adding shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...")
        consumer.wakeup()
      }

      // Join the main thread to allow the execution of the code in the main thread
      try {
        mainThread.join()
      } catch {
        case e: InterruptedException =>
          println("InterruptedException caught:")
          e.printStackTrace()
      }
    })

    try{
      log.info("Working until this")
      // Subscribe to a topic
      consumer.subscribe(util.Arrays.asList("secondTopic"))

      // Poll for data
      while (true) {
        log.info("Polling")

        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(1000))

        for (record <- records.asScala) {
          println(s"Key: ${record.key()}, Value: ${record.value()}")
          println(s"Partition: ${record.partition()}, Offset: ${record.offset()}")
        }
      }
    }
    catch{
      case e: WakeupException =>
        println("Consumer is starting to shutdown")
      case e: Exception =>
        println("Unexpected exception in the consumer "+ e)
    } finally{
      consumer.close()
      log.info("The consumer is now gracefully shutdown")
    }
  }
}
