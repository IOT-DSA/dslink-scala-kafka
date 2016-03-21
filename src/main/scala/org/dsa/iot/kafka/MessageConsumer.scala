package org.dsa.iot.kafka

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import org.dsa.iot.kafka.Settings.consumerOptions
import org.slf4j.LoggerFactory

import KafkaUtils.{ findLeader, getEarliestOffset, getLatestOffset, parseBrokerList }
import kafka.api.{ FetchRequestBuilder, FetchResponse, OffsetRequest }
import kafka.consumer.SimpleConsumer

/**
 * Kafka message consumer.
 */
class MessageConsumer(val brokerUrl: String, val topic: String, val partition: Int, offset: Long) {
  import Settings.consumerOptions._

  private val log = LoggerFactory.getLogger(getClass)

  private val brokers = parseBrokerList(brokerUrl)

  private val flag = new AtomicBoolean(false)
  private var latch: CountDownLatch = null
  private var consumer: SimpleConsumer = null

  // determine the initial offset
  var readOffset = offset match {
    case OffsetRequest.EarliestTime => retry(10)(getEarliestOffset(brokers, topic, partition))
    case OffsetRequest.LatestTime   => retry(10)(getLatestOffset(brokers, topic, partition))
    case _                          => offset
  }
  log.info("Initial offset retrieved: " + readOffset)

  /**
   * Starts streaming from Kafka, notifying the listener on each read batch.
   */
  def start(listener: (Array[Byte], Long) => Unit) = {
    var maxSize = fetchSize

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

      if (messages.isEmpty && rsp.sizeInBytes > maxSize) {
        log.warn(s"Message size of ${rsp.sizeInBytes} exceeds the maximum size $maxSize, adjusting")
        maxSize = math.max(rsp.sizeInBytes, maxSize * 2)
      }

      if (messages.size > 100) {
        log.warn(s"Message size may be too large, adjusting")
        maxSize = math.max(fetchSize, rsp.sizeInBytes / 5)
      }

      if (messages.isEmpty)
        Thread.sleep(delayOnEmpty)
    }

    // start streaming
    flag.set(true)
    recreateConsumer
    latch = new CountDownLatch(1)
    while (flag.get) {
      val fetchRequest = new FetchRequestBuilder()
        .clientId(clientId)
        .addFetch(topic, partition, readOffset, maxSize)
        .build

      log.debug(s"Preparing to fetch from $topic:$partition at $readOffset")
      val fetchResponse = Try(consumer.fetch(fetchRequest)).filter(!_.hasError)

      fetchResponse match {
        case Success(fr) => processResponse(fr)
        case Failure(e)  => recreateConsumer
      }
    }

    latch.countDown
  }

  /**
   * Stops streaming from Kafka.
   */
  def stop() = {
    log.info(s"Stopping streaming from $topic:$partition")
    flag.set(false)
    latch.await
    log.info(s"Streaming from $topic:$partition stopped")
  }

  /**
   * Closes the existing consumer and creates a new one.
   */
  private def recreateConsumer() = {
    log.info(s"Preparing to (re-)create the consumer for $topic:$partition")

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
    log.info(s"Trying to create a new consumer for $topic:$partition..")
    consumer = retry(20, 10)(createConsumer)
    log.info(s"New consumer for $topic:$partition successfully created")
  }

  /**
   * Finds the cluster leader and creates a new Kafka SimpleConsumer.
   */
  private def createConsumer = {
    log.info(s"Searching for leader in hosts $brokerUrl topic $topic partition $partition")
    val leader = findLeader(brokers, topic, partition) getOrElse (
      throw new IllegalArgumentException(s"Can't find leader for hosts $brokerUrl topic $topic partition $partition"))
    log.info(s"Kafka leader found: $leader")

    log.info(s"Creating a Kafka consumer: host=${leader.host} port=${leader.port} timeOut=$timeOut bufferSize=$bufferSize clientId=$clientId")
    val sc = new SimpleConsumer(leader.host, leader.port, timeOut, bufferSize, clientId)
    log.info(s"Kafka consumer created: host=${sc.host} port=${sc.port} timeOut=$timeOut bufferSize=$bufferSize clientId=$clientId")
    sc
  }
}