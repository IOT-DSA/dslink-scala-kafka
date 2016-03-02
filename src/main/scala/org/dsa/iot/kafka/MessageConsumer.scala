package org.dsa.iot.kafka

import kafka.consumer.SimpleConsumer
import kafka.api.FetchRequestBuilder
import java.util.concurrent.atomic.AtomicBoolean
import org.slf4j.LoggerFactory
import kafka.common.ErrorMapping
import kafka.api.FetchResponse
import scala.util.control.NonFatal
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * Kafka message consumer.
 */
class MessageConsumer(val hosts: Iterable[String], val topic: String, val partition: Int, offset: Long) {
  import Settings.consumerOptions._

  private val log = LoggerFactory.getLogger(getClass)

  private val flag = new AtomicBoolean(false)

  // create Kafka consumer
  var consumer = createConsumer

  // determine the initial offset
  var readOffset =
    if (offset >= 0) offset
    else KafkaUtils.getLastOffset(consumer, topic, partition, offset, clientId)
  log.info("Initial offset retrieved: " + readOffset)

  /**
   * Starts streaming from Kafka, notifying the listener on each read batch.
   */
  def start(listener: (Array[Byte], Long) => Unit) = {

    // Extracts messages from the response and notifies the listener.
    def processResponse(rsp: FetchResponse) = {
      val messages = rsp.messageSet(topic, partition)
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

          listener(data, readOffset)
        }
      }

      if (messages.isEmpty)
        Thread.sleep(delayOnEmpty)
    }

    // start streaming
    flag.set(true)
    while (flag.get) {
      val fetchRequest = new FetchRequestBuilder()
        .clientId(clientId)
        .addFetch(topic, partition, readOffset, fetchSize)
        .build

      val fetchResponse = Try(consumer.fetch(fetchRequest)).filter(!_.hasError)

      fetchResponse match {
        case Success(fr) => processResponse(fr)
        case Failure(e)  => recreateConsumer
      }
    }
  }

  /**
   * Stops streaming from Kafka.
   */
  def stop() = flag.set(false)

  /**
   * Closes the existing consumer and creates a new one.
   */
  private def recreateConsumer() = {
    // close old consumer
    if (consumer != null) try {
      consumer.close
      log.info("Old consumer closed")
    } catch {
      case NonFatal(e) => log.error("Error closing consumer", e)
    } finally {
      consumer = null
    }

    // wait for things to cool down
    Thread.sleep(1000)

    // try to reestablish connection
    consumer = retry(20, 100)(createConsumer)
  }

  /**
   * Finds the cluster leader and creates a new Kafka SimpleConsumer.
   */
  private def createConsumer = {
    log.debug(s"Searching for leader in hosts $hosts topic $topic partition $partition")
    val leader = KafkaUtils.findLeader(hosts, topic, partition) getOrElse (
      throw new IllegalArgumentException(s"Can't find leader for hosts $hosts topic $topic partition $partition"))
    log.info(s"Kafka leader found: $leader")

    log.debug(s"Creating a Kafka consumer: host=${leader.host} port=${leader.port} timeOut=$timeOut bufferSize=$bufferSize clientId=$clientId")
    val sc = new SimpleConsumer(leader.host, leader.port, timeOut, bufferSize, clientId)
    log.info(s"Kafka consumer created: host=${sc.host} port=${sc.port} timeOut=$timeOut bufferSize=$bufferSize clientId=$clientId")
    sc
  }
}