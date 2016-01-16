package org.dsa.iot.kafka

import scala.annotation.migration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.actions.ActionResult
import org.dsa.iot.dslink.node.value.ValueType
import org.slf4j.LoggerFactory

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }

/**
 * Consumer offset type.
 */
object OffsetType extends Enumeration {
  type OffsetType = Value
  val Earliest, Latest, Custom = Value
}
import OffsetType._

/**
 * Application controller.
 */
class AppController(root: Node) {
  import Settings._
  import ValueType._

  private val log = LoggerFactory.getLogger(getClass)

  initRoot

  log.info("Application controller started")

  /**
   * Initializes the root node.
   */
  private def initRoot() = {
    root createChild "addConnection" display "Add Connection" action addConnection build ()

    root.children.values filter (_.nodeType == Some("connection")) foreach initConnNode
  }

  /**
   * Initializes connection node.
   */
  private def initConnNode(node: Node) = {
    val name = node.getName
    val brokerUrl = node.getAttribute("brokerUrl").getString

    node createChild "removeConnection" display "Remove Connection" action removeConnection build ()
    node createChild "publish" display "Publish" action publish build ()
    node createChild "addSubscription" display "Subscribe" action subscribe build ()

    node.children.values filter (_.nodeType == Some("topic")) foreach initTopicNode
  }

  /**
   * Initializes topic node.
   */
  private def initTopicNode(node: Node) = {
    node createChild "removeSubscription" display "Unsubscribe" action unsubscribe build ()

    node.children.values filter (_.nodeType == Some("partition")) foreach initPartitionNode
  }

  /**
   * Initializes partition node.
   */
  private def initPartitionNode(node: Node) = {
    val topicNode = node.getParent
    val topic = topicNode.getName

    val brokerUrl = topicNode.getParent.getAttribute("brokerUrl").getString

    val partition = node.getAttribute("partition").getNumber.intValue
    val offsetType = OffsetType withName node.getAttribute("offsetType").getString
    val customOffset = node.getAttribute("customOffset").getNumber.longValue
    val emitDelay = node.getAttribute("emitDelay").getNumber.longValue

    log.debug(s"Searching for leader for broker $brokerUrl topic $topic partition $partition")
    val hosts = brokerUrl split "," map (_.trim)
    val leader = KafkaUtils.findLeader(hosts, topic, partition) getOrElse (
      throw new IllegalArgumentException("Can't find leader for topic and partition"))
    log.info(s"Kafka leader found: $leader")

    val offset = offsetType match {
      case Earliest => kafka.api.OffsetRequest.EarliestTime
      case Latest   => kafka.api.OffsetRequest.LatestTime
      case _                   => customOffset
    }

    val consumer = new MessageConsumer(leader.host, leader.port, topic, partition, offset)
    node.setMetaData(consumer)

    future {
      log.info(s"Streaming started for $topic/$partition with offset $offsetType")
      consumer.start { (data, currentOffset) =>
        log.debug(s"Message received: ${data.length} bytes")
        node.setValue(anyToValue(new String(data)))
        node.setAttribute("offsetType", anyToValue(Custom.toString))
        node.setAttribute("customOffset", anyToValue(currentOffset))
        if (emitDelay > 0)
          Thread.sleep(emitDelay)
      }
    }
  }

  /* actions */

  def addConnection = action(
    parameters = List(
      STRING("name") default "new_connection" description "Connection name",
      STRING("brokers") default DEFAULT_BROKER_URL description "Hosts with optional :port suffix"),
    handler = (event: ActionResult) => {
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val brokerUrl = event.getParam[String]("brokers", !_.isEmpty, "Brokers cannot be empty").trim
      if (root.children.contains(name))
        throw new IllegalArgumentException(s"Duplicate connection name: $name")

      log.info(s"Adding new connection '$name' for '$brokerUrl'")
      val connNode = root createChild name nodeType "connection" attributes "brokerUrl" -> anyToValue(brokerUrl) build ()
      connNode createChild "brokerUrl" display "Broker URL" valueType STRING value anyToValue(brokerUrl) build ()

      initConnNode(connNode)
    })

  def removeConnection = (event: ActionResult) => {
    val node = event.getNode.getParent
    val name = node.getName

    val nonEmpty = node.children.values.exists(_.nodeType == Some("topic"))
    if (nonEmpty)
      throw new IllegalArgumentException(s"There are subscriptions for connection $name")

    node.delete
    log.info(s"Connection '$name' removed")
  }

  def publish = action(
    parameters = List(STRING("topic"), STRING("message")),
    handler = (event: ActionResult) => {
      val connNode = event.getNode.getParent
      val brokerUrl = connNode.getAttribute("brokerUrl").getString

      val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
      val message = event.getParam[String]("message", !_.isEmpty, "Message cannot be empty")

      log.info(s"Publishing message onto $brokerUrl/$topic")

      val props = producerOptions.newProperties
      props.put("metadata.broker.list", brokerUrl)

      val cfg = new ProducerConfig(props)

      val p = new Producer[String, String](cfg)
      val data = new KeyedMessage[String, String](topic, message)
      p.send(data)
      p.close
    })

  def subscribe = action(
    parameters = List(
      STRING("topic"),
      NUMBER("partition") default 0,
      ENUMS(OffsetType)("offsetType") default "Latest",
      NUMBER("offset") description "Custom offset" default 0,
      NUMBER("emitDelay") description "Delay between two messages" default 0),
    handler = (event: ActionResult) => {
      val connNode = event.getNode.getParent
      val brokerUrl = connNode.getAttribute("brokerUrl").getString

      val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
      val partition = event.getParam[Number]("partition", _.intValue >= 0, "Invalid partition").intValue
      val offsetType = OffsetType withName event.getParam[String]("offsetType")
      val customOffset = event.getParam[Number]("offset").longValue
      val emitDelay = event.getParam[Number]("emitDelay").longValue

      val exists = connNode.children.get(topic).flatMap(_.children.get(s"#$partition")).isDefined
      if (exists)
        throw new IllegalArgumentException(s"Duplicate subscription for topic $topic and partition $partition")

      val topicNode = connNode.children getOrElse (topic, connNode createChild topic nodeType "topic" build ())

      val partitionNode = topicNode createChild s"#$partition" valueType STRING nodeType "partition" attributes
        ("partition" -> anyToValue(partition), "offsetType" -> anyToValue(offsetType.toString),
          "customOffset" -> anyToValue(customOffset), "emitDelay" -> anyToValue(emitDelay)) build ()

      initPartitionNode(partitionNode)
    })

  def unsubscribe = action(
    parameters = List(NUMBER("partition") default 0),
    handler = (event: ActionResult) => {
      val partition = event.getParam[Number]("partition", _.intValue >= 0, "Invalid partition").intValue

      val topicNode = event.getNode.getParent
      val partitionNode = topicNode.children(s"#$partition")

      val consumer = partitionNode.getMetaData[MessageConsumer]
      if (consumer != null)
        future { consumer.stop }

      topicNode.removeChild(partitionNode)
      if (topicNode.children.count(_._2.nodeType == Some("partition")) == 0)
        topicNode.delete

      log.info(s"Subscription ${topicNode.getName}/$partition removed")
    })
}