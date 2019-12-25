package kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{LoggerFactory}

import scala.util.{Failure, Success, Try}

case class Producer( bootStrapServerList: String ) {
    //LOGGER
    val logger = LoggerFactory.getLogger(Producer.getClass)

    // CREATE PRODUCER PROPERTIES
    private val properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServerList)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // CREATE THE KAFKA PRODUCER
    private val producer = new KafkaProducer[String, String](properties)

    //SEND RECORD - ASYNC
    def WriteToKafka(topic: String, message: String)  = {
        Try {
            // CREATE A PRODUCER RECORD
            val record = new ProducerRecord[String, String](topic, message)
            val response = producer.send(record)
            producer.flush()
            response
        } match {
            case Success(metadata) => logger.info("Recevied meta data \n" +
            "Topic: " + metadata.get.topic + "\n" +
            "Partition: " + metadata.get.partition + "\n" +
            "Offset: " + metadata.get.offset + "\n" +
            "Timestamp: " + metadata.get.timestamp + "\n"
            )
            case Failure(exception) => logger.error("error",exception.printStackTrace)
        }
    }
}
