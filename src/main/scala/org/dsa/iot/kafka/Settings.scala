package org.dsa.iot.kafka

import com.typesafe.config.ConfigFactory
import java.util.Properties

object Settings {
  private lazy val config = ConfigFactory.load

  val DEFAULT_HOST = "127.0.0.1"
  val DEFAULT_PORT = 9092
  val DEFAULT_BROKER_URL = DEFAULT_HOST + ":" + DEFAULT_PORT

  object consumerOptions {
    private val node = config.getConfig("consumer")

    val timeOut = node.getInt("timeOut")
    val bufferSize = node.getInt("bufferSize")
    val fetchSize = node.getInt("fetchSize")
    val clientId = node.getString("clientId")
    val delayOnEmpty = node.getInt("delayOnEmpty")
  }

  object producerOptions {
    private val node = config.getConfig("producer")

    val serializerClass = node.getString("serializerClass")
    val partitionerClass = node.getString("partitionerClass")
    val requestRequiredAcks = node.getInt("requestRequiredAcks")

    def newProperties = {
      val props = new Properties
      props.put("serializer.class", serializerClass)
      props.put("partitioner.class", partitionerClass)
      props.put("request.required.acks", requestRequiredAcks.toString)
      props
    }
  }
}