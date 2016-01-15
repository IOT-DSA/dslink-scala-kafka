package org.dsa.iot.kafka

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

import org.dsa.iot.dslink.{ DSLink, DSLinkFactory, DSLinkHandler }
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.{ Value, ValueType }
import org.slf4j.LoggerFactory

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }

/**
 * Kafka DSLink.
 */
object Main extends DSLinkHandler with Controller {
  import Settings._
  import ValueType._

  private val log = LoggerFactory.getLogger(getClass)

  private var root: Node = null

  /* DSLinkHandler API */

  override val isResponder = true

  override def onResponderInitialized(link: DSLink) = {
    log.info("Responder initialized")
    init(link)
  }

  /* Controller API */

  var connections = Map.empty[String, Connection]
  var subscriptions = Map.empty[SubscriptionKey, Subscription]

  /**
   * Creates a new Kafka connection.
   */
  def addConnection(name: String, brokerUrl: String) = {
    log.info(s"Adding new connection '$name' for '$brokerUrl'")
    val brokerUrlVal = new Value(brokerUrl)
    val connNode = root.createChild(name).build
    connNode.createChild("brokerUrl").setDisplayName("Broker URL").setValueType(STRING).setValue(brokerUrlVal).build

    val addSubcsriptionNode = connNode
      .createChild("addSubscription")
      .setDisplayName("Subscribe")
      .setAction(Actions.addSubscriptionAction(name, this))
      .build

    val publishNode = connNode
      .createChild("publish")
      .setDisplayName("Publish")
      .setAction(Actions.publishAction(brokerUrl, this))
      .build

    val removeConnectionNode = connNode
      .createChild("removeConnection")
      .setDisplayName("Remove Connection")
      .setAction(Actions.removeConnectionAction(name, this))
      .build

    val connection = Connection(name, brokerUrl, connNode)
    connections += name -> connection
    connection
  }

  /**
   * Removes a connection.
   */
  def removeConnection(name: String) = {
    connections -= name
    root.removeChild(name)
    log.info(s"Connection '$name' removed")
  }

  /**
   * Publishes a message onto a Kafka topic
   */
  def publish(brokerUrl: String, topic: String, message: String) = {
    log.info(s"Publishing message onto $brokerUrl/$topic")

    val props = producerOptions.newProperties
    props.put("metadata.broker.list", brokerUrl)

    val cfg = new ProducerConfig(props)

    val p = new Producer[String, String](cfg)
    val data = new KeyedMessage[String, String](topic, message)
    p.send(data)
    p.close
  }

  /**
   * Adds a subscription and starts streaming data.
   */
  def addSubscription(connName: String, topic: String, partition: Int, offsetType: OffsetType.OffsetType, customOffset: Long) = {
    val connection = connections(connName)
    val brokerUrl = connection.brokerUrl

    log.debug(s"Searching for leader for broker $brokerUrl topic $topic partition $partition")
    val hosts = brokerUrl split "," map (_.trim)
    val leader = KafkaUtils.findLeader(hosts, topic, partition) getOrElse (
      throw new IllegalArgumentException("Can't find leader for topic and partition"))
    log.info(s"Kafka leader found: $leader")

    log.info(s"Adding new subscription for $topic/$partition with offset $offsetType")
    val connNode = connection.node
    val topicNode = connNode.getChildren.asScala.getOrElse(topic, connNode.createChild(topic).build)
    val partitionNode = topicNode.createChild(s"#$partition").setValueType(ValueType.STRING).build

    val offset = offsetType match {
      case OffsetType.Earliest => kafka.api.OffsetRequest.EarliestTime
      case OffsetType.Latest   => kafka.api.OffsetRequest.LatestTime
      case _                   => customOffset
    }

    val consumer = new MessageConsumer(leader.host, leader.port, topic, partition, offset)

    val subscription = Subscription(connName, topic, partition, offsetType, customOffset, partitionNode, consumer)
    subscriptions += subscription.key -> subscription

    val removeSubscriptionNode = topicNode
      .createChild("removeSubscription")
      .setDisplayName("Unsubscribe")
      .setAction(Actions.removeSubscriptionAction(connName, topic, this))
      .build

    future {
      log.info(s"Streaming started for $topic/$partition with offset $offsetType")
      consumer.start { data =>
        log.debug(s"Message received: ${data.length} bytes")
        partitionNode.setValue(new Value(new String(data)))
        if (consumerOptions.emitDelay > 0)
          Thread.sleep(consumerOptions.emitDelay)
      }
    }

    subscription
  }

  /**
   * Removes a subscription.
   */
  def removeSubscription(connName: String, topic: String, partition: Int) = {
    val key = SubscriptionKey(connName, topic, partition)
    val subscription = subscriptions(key)
    future { subscription.consumer.stop }
    subscriptions -= key

    val partitionNode = subscription.node
    val topicNode = partitionNode.getParent
    topicNode.removeChild(partitionNode)
    if (topicNode.getChildren.size < 2)
      topicNode.getParent.removeChild(topicNode)

    log.info(s"Subscription $connName/$topic/$partition removed")
  }

  /* implementation */

  private def init(link: DSLink) = {
    root = link.getNodeManager.getSuperRoot

    val addConnectionNode = root
      .createChild("addConnection")
      .setDisplayName("Add Connection")
      .setAction(Actions.addConnectionAction(this))
      .build
  }

  /**
   * DSLink entry point.
   */
  def main(args: Array[String]): Unit = DSLinkFactory.start(args, this)
}