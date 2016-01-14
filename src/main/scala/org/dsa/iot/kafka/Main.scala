package org.dsa.iot.kafka

import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.dsa.iot.dslink.{ DSLink, DSLinkFactory, DSLinkHandler }
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.Value
import org.slf4j.LoggerFactory
import kafka.common.TopicAndPartition
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import org.dsa.iot.dslink.node.value.ValueType

trait Controller {
  def addConnection(parent: Node, name: String, brokerUrl: String): Unit
  def removeConnection(node: Node): Unit
  def publish(brokerUrl: String, topic: String, message: String): Unit
  def consumers: Map[String, Map[TopicAndPartition, MessageConsumer]]
  def addSubcription(parent: Node, topic: String, partition: Int, offsetType: String, offset: Long): Unit
}

/**
 * Kafka DSLink.
 */
object Main extends DSLinkHandler with Controller {
  import Settings._

  private val log = LoggerFactory.getLogger(getClass)

  var consumers = Map.empty[String, Map[TopicAndPartition, MessageConsumer]]

  /* DSLinkHandler API */

  override val isResponder = true

  override def onResponderInitialized(link: DSLink) = {
    log.info("Responder initialized")
    init(link)
  }

  /* implementation */

  private def init(link: DSLink) = {
    val root = link.getNodeManager.getSuperRoot

    val addConnectionNode = root
      .createChild("addConnection")
      .setDisplayName("Add Connection")
      .setAction(Actions.addConnectionAction(root, this))
      .build
  }

  /**
   * Creates a new Kafka connection node.
   */
  def addConnection(parent: Node, name: String, brokerUrl: String) = {
    val connNode = parent.createChild(name).setAttribute("brokerUrl", new Value(brokerUrl)).build
    connNode.createChild("brokerUrl").setDisplayName("Broker URL")
      .setValueType(ValueType.STRING).setValue(new Value(brokerUrl)).build

    val addSubcsriptionNode = connNode
      .createChild("addSubscription")
      .setDisplayName("Subscribe")
      .setAction(Actions.addSubscriptionAction(connNode, this))
      .build

    val publishNode = connNode
      .createChild("publish")
      .setDisplayName("Publish")
      .setAction(Actions.publishAction(connNode, this))
      .build

    val removeConnectionNode = connNode
      .createChild("removeConnection")
      .setDisplayName("Remove Connection")
      .setAction(Actions.removeConnectionAction(connNode, this))
      .build
  }

  def removeConnection(node: Node) = node.getParent.removeChild(node)

  /**
   * Publishes a message onto a Kafka topic
   */
  def publish(brokerUrl: String, topic: String, message: String) = {
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
  def addSubcription(parent: Node, topic: String, partition: Int, offsetType: String, customOffset: Long) = {

    val brokerUrl = parent.getAttribute("brokerUrl")
    log.debug(s"Searching for leader for broker $brokerUrl topic $topic partition $partition")
    val hosts = brokerUrl.getString split "," map (_.trim)
    val leader = KafkaUtils.findLeader(hosts, topic, partition) getOrElse (
      throw new IllegalArgumentException("Can't find leader for topic and partition"))
    log.info(s"Kafka leader found: $leader")

    val topicNode = parent.getChildren.asScala.getOrElse(topic, parent.createChild(topic).build)

    val partitionNode = topicNode.createChild(s"#$partition").setValueType(ValueType.STRING).build

    val offset = offsetType match {
      case "Earliest" => kafka.api.OffsetRequest.EarliestTime
      case "Latest"   => kafka.api.OffsetRequest.LatestTime
      case _          => customOffset
    }

    val consumer = new MessageConsumer(leader.host, leader.port, topic, partition, customOffset)

    val tpmc = consumers.getOrElse(parent.getName, Map.empty[TopicAndPartition, MessageConsumer]) + (TopicAndPartition(topic, partition) -> consumer)
    consumers += parent.getName -> tpmc

    val removeSubscriptionNode = topicNode
      .createChild("removeSubscription")
      .setDisplayName("Unsubscribe")
      .setAction(Actions.removeSubscriptionAction(partitionNode, this))
      .build

    consumer.start { data =>
      log.debug(s"Message received: ${data.length} bytes")
      partitionNode.setValue(new Value(new String(data)))
    }
  }

  /**
   * DSLink entry point.
   */
  def main(args: Array[String]): Unit = DSLinkFactory.start(args, this)
}