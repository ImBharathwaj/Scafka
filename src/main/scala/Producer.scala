package org.tamaai.com
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import java.util.Properties

object Producer {
  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  def SendRecords(): Unit = {
    log.info("This is working!");

    var properties: Properties = new Properties()

    //  Create producer properties
    properties.setProperty("bootstrap.servers","localhost:9092")

    // set serializer
    properties.setProperty("key.serializer",classOf[StringSerializer].getName)
    properties.setProperty("value.serializer",classOf[StringSerializer].getName)

    // Create Kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // Create producer records
    val producerRecords = new ProducerRecord[String, String]("firstTopic", "Value")

    // Send data
    producer.send(producerRecords)

    // Tell the producer to send all the data and block until done -- synchronize
    producer.flush()

    // flush and close the producer
    producer.close()
  }
}
