package org.dsa.iot.kafka

import scala.util.Random

import org.dsa.iot.kafka.Settings.consumerOptions

import kafka.api.{ OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest }
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.threadsafe

/**
 * Kafka Utility functions.
 */
object KafkaUtils {
  import Settings.consumerOptions._

  /**
   * Finds the leader broker.
   */
  def findLeader(brokers: Iterable[String], topic: String, partition: Int) = {

    def parseHostAndPort(broker: String) = broker.split(":") match {
      case Array(h, p) => (h, p.toInt)
      case _           => (broker, Settings.DEFAULT_PORT)
    }

    val consumers = brokers map { broker =>
      val (host, port) = parseHostAndPort(broker)
      new SimpleConsumer(host, port, timeOut, bufferSize, "leaderLookup")
    }

    val req = new TopicMetadataRequest(List(topic), Random.nextInt)

    val metadata = for {
      consumer <- consumers
      tmd <- consumer.send(req).topicsMetadata
      pmd <- tmd.partitionsMetadata if pmd.partitionId == partition
    } yield pmd

    consumers foreach (_.close)

    metadata.headOption flatMap (_.leader)
  }

  /**
   * Returns the last found offset for the topic/partition.
   */
  def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, time: Long, clientId: String) = {
    val tap = TopicAndPartition(topic, partition)
    val requestInfo = Map(tap -> PartitionOffsetRequestInfo(time, 1))
    val req = OffsetRequest(requestInfo = requestInfo, clientId = clientId)
    val rsp = consumer.getOffsetsBefore(req)
    if (rsp.hasError)
      throw new IllegalArgumentException("Error fetching data Offset Data the Broker: " + rsp.describe(true))
    else
      rsp.partitionErrorAndOffsets(tap).offsets(0)
  }
}