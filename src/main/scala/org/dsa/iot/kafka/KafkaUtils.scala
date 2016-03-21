package org.dsa.iot.kafka

import scala.util.Random

import org.dsa.iot.kafka.Settings.consumerOptions

import kafka.api.{ OffsetRequest, PartitionOffsetRequestInfo, TopicMetadata, TopicMetadataResponse }
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.{ TopicAndPartition, UnknownTopicOrPartitionException }
import kafka.common.ErrorMapping
import kafka.common.ErrorMapping._
import kafka.consumer.SimpleConsumer
import kafka.utils.threadsafe

/**
 * Kafka Utility functions.
 */
object KafkaUtils {
  import Settings.consumerOptions._

  /**
   * Similar to {{kafka.client.ClientUtils.parseBrokerList}} but allows empty ports, in which case
   * the default port will be used.
   *
   * @see {{Settings.DEFAULT_PORT}}.
   */
  def parseBrokerList(brokerUrl: String): Seq[Broker] = {

    def parseHostAndPort(broker: String) = broker.split(":") match {
      case Array(h, p) => (h.trim, p.trim.toInt)
      case _           => (broker.trim, Settings.DEFAULT_PORT)
    }

    val list = brokerUrl split ("\\s*,\\s*") map (_.trim) filterNot (_.isEmpty)
    list.zipWithIndex map {
      case (str, id) =>
        val (host, port) = parseHostAndPort(str)
        Broker(id, host, port)
    }
  }

  /**
   * A wrapper for {{kafka.client.ClientUtils.fetchTopicMetadata}}.
   */
  def fetchTopicMetadata(brokers: Seq[Broker], topics: Set[String]): TopicMetadataResponse =
    ClientUtils.fetchTopicMetadata(topics, brokers, clientId, timeOut, Random.nextInt)

  /**
   * Calls {{fetchTopicMetadata}} for a single topic.
   */
  def fetchTopicMetadata(brokers: Seq[Broker], topic: String): TopicMetadata =
    fetchTopicMetadata(brokers, Set(topic)).topicsMetadata.head

  /**
   * Finds the leader broker for a given topic and partition.
   */
  def findLeader(brokers: Seq[Broker], topic: String, partition: Int): Option[Broker] =
    fetchTopicMetadata(brokers, topic).partitionsMetadata.headOption.flatMap(_.leader)

  /**
   * Returns the last offset before the specified epoch time.
   */
  def getOffsetBefore(brokers: Seq[Broker], topic: String, partition: Int, time: Long): Option[Long] = {
    val ti = TopicAndPartition(topic, partition)
    val reqInfo = Map(ti -> PartitionOffsetRequestInfo(time, 1))
    val request = OffsetRequest(requestInfo = reqInfo, clientId = clientId)

    for {
      leader <- findLeader(brokers, topic, partition)
      consumer = new SimpleConsumer(leader.host, leader.port, timeOut, bufferSize, clientId)
      rsp <- consumer.getOffsetsBefore(request).partitionErrorAndOffsets.values.headOption
      offset <- rsp.offsets.headOption
    } yield offset
  }

  /**
   * Returns the earliest available offset.
   */
  def getEarliestOffset(brokers: Seq[Broker], topic: String, partition: Int): Long =
    getOffsetBefore(brokers, topic, partition, OffsetRequest.EarliestTime) getOrElse {
      throw new UnknownTopicOrPartitionException
    }

  /**
   * Returns the latest available offset.
   */
  def getLatestOffset(brokers: Seq[Broker], topic: String, partition: Int): Long =
    getOffsetBefore(brokers, topic, partition, OffsetRequest.LatestTime) getOrElse {
      throw new UnknownTopicOrPartitionException
    }

  /**
   * Returns the error description.
   */
  def getErrorInfo(errorCode: Int): String = errorCode match {
    case NoError                             => "None"
    case OffsetOutOfRangeCode                => "Offset out of range"
    case InvalidMessageCode                  => "Invalid message"
    case UnknownTopicOrPartitionCode         => "Unknown topic or partition"
    case InvalidFetchSizeCode                => "Invalid fetch size"
    case LeaderNotAvailableCode              => "Leader not available"
    case NotLeaderForPartitionCode           => "No leader for partition"
    case RequestTimedOutCode                 => "Request timed out"
    case BrokerNotAvailableCode              => "Broker not available"
    case ReplicaNotAvailableCode             => "Replica not available"
    case MessageSizeTooLargeCode             => "Message size too large"
    case StaleControllerEpochCode            => "Stale controller"
    case OffsetMetadataTooLargeCode          => "Offset metadata too large"
    case StaleLeaderEpochCode                => "Stale leader epoch"
    case OffsetsLoadInProgressCode           => "Offsets load in progress"
    case ConsumerCoordinatorNotAvailableCode => "Consumer coordinator not available"
    case NotCoordinatorForConsumerCode       => "No coordinator for consumer"
    case InvalidTopicCode                    => "Invalid topic code"
    case MessageSetSizeTooLargeCode          => "Message set size too large"
    case NotEnoughReplicasCode               => "No enough replicas"
    case NotEnoughReplicasAfterAppendCode    => "No enough replicas after append"
    case _                                   => "Unknown"
  }

  /**
   * Returns the error description.
   */
  def getErrorInfo(error: Throwable): String = getErrorInfo(ErrorMapping.codeFor(error.getClass.asInstanceOf[Class[Throwable]]))

  /**
   * Formats a broker info.
   */
  def formatBroker(broker: Broker) = broker.host + ":" + broker.port
}