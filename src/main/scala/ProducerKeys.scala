package org.tamaai.com

import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object ProducerKeys {
  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  def SendRecordsWithKey(): Unit = {
    log.info("This is not working!");

    val properties: Properties = new Properties()

    //  Create producer properties
    properties.setProperty("bootstrap.servers","localhost:9092")

    // set serializer
    properties.setProperty("key.serializer",classOf[StringSerializer].getName)
    properties.setProperty("value.serializer",classOf[StringSerializer].getName)

    //    properties.setProperty("batch.size", "400")
    //    properties.setProperty("partition.class", classOf[RoundRobinPartitioner].getName)

    // Create Kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // Create producer records
    for (j <- 1 to 2) {
      for (i <- 1 to 10) {
        val topic = "secondTopic"
        val key = "id_" + i
        val value = "Hello World " + i
        val producerRecords = new ProducerRecord[String, String](topic, key, value)

        // Send data
        producer.send(producerRecords, new Callback() {
          @Override
          def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
            // execute every time a record successfully sent
            if (e == null) {
              log.info("Key: " + key + " | Partition: " + metadata.partition())
            } else {
              log.error("Error while producing: " + e)
            }
          }
        })
      }
      Thread.sleep(500)
    }

    // Tell the producer to send all the data and block until done -- synchronize
    producer.flush()

    // flush and close the producer
    producer.close()
  }
}