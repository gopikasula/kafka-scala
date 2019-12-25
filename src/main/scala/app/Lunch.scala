package app

import kafka.producer.Producer

object Lunch extends App {
  val producer = Producer("0.0.0.0:9092")
  producer.WriteToKafka("first_topic","hello kafka")
}
