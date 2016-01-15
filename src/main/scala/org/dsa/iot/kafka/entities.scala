package org.dsa.iot.kafka

import org.dsa.iot.dslink.node.Node

/**
 * Kafka connection.
 */
case class Connection(name: String, brokerUrl: String, node: Node)

/**
 * Consumer offset type.
 */
object OffsetType extends Enumeration {
  type OffsetType = Value
  val Earliest, Latest, Custom = Value
}

/**
 * Kafka topic/partition subscription.
 */
case class Subscription(connName: String, topic: String, partition: Int,
                        offsetType: OffsetType.OffsetType, customOffset: Long,
                        node: Node, consumer: MessageConsumer) {
  val key = SubscriptionKey(connName, topic, partition)
}

/**
 * The indexing key for subscriptions.
 */
case class SubscriptionKey(connName: String, topic: String, partition: Int)

/**
 * Application controller.
 */
trait Controller {
  def connections: Map[String, Connection]
  def addConnection(name: String, brokerUrl: String): Connection
  def removeConnection(name: String): Unit

  def publish(brokerUrl: String, topic: String, message: String): Unit

  def subscriptions: Map[SubscriptionKey, Subscription]
  def addSubscription(connName: String, topic: String, partition: Int,
                      offsetType: OffsetType.OffsetType, customOffset: Long): Subscription
  def removeSubscription(connName: String, topic: String, partition: Int): Unit
}