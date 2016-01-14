package org.dsa.iot.kafka

import kafka.consumer.SimpleConsumer
import kafka.api.FetchRequestBuilder
import java.util.concurrent.atomic.AtomicBoolean
import org.slf4j.LoggerFactory

/**
 * Kafka message consumer.
 */
class MessageConsumer(val host: String, val port: Int, val topic: String, val partition: Int, offset: Long) {
  import Settings.consumerOptions._

  private val log = LoggerFactory.getLogger(getClass)

  val consumer = new SimpleConsumer(host, port, timeOut, bufferSize, clientId)
  log.info(s"Kafka consumer created: host=$host port=$port timeOut=$timeOut bufferSize=$bufferSize clientId=$clientId")

  val flag = new AtomicBoolean(false)

  var readOffset =
    if (offset >= 0) offset
    else KafkaUtils.getLastOffset(consumer, topic, partition, offset, clientId)
  log.info("Initial offset retrieved: " + readOffset)

  def start(listener: Array[Byte] => Unit) = {
    flag.set(true)

    while (flag.get) {
      val fetchRequest = new FetchRequestBuilder()
        .clientId(clientId)
        .addFetch(topic, partition, readOffset, fetchSize)
        .build

      val fetchResponse = consumer.fetch(fetchRequest)

      if (fetchResponse.hasError)
        throw new RuntimeException(s"Error fetching data: ${fetchResponse.describe(true)}")

      val messages = fetchResponse.messageSet(topic, partition)
      messages.buffer.flip.limit(messages.buffer.capacity)
      log.debug(s"$topic: ${messages.size} messages received in a batch")

      messages foreach { mao =>
        val currentOffset = mao.offset
        if (currentOffset < readOffset)
          log.warn(s"Found an old offset $currentOffset, expecting $readOffset")
        else {
          readOffset = mao.nextOffset

          val payload = mao.message.payload
          val data = Array.fill[Byte](payload.limit)(0)
          payload.get(data)

          listener(data)
        }
      }

      if (messages.isEmpty)
        Thread.sleep(delayOnEmpty)
    }
  }

  def stop() = flag.set(false)
}