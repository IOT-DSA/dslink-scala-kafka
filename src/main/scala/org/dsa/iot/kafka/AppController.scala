package org.dsa.iot.kafka

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future
import scala.util.control.NonFatal
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.actions.ActionResult
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.ValueType
import org.slf4j.LoggerFactory
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import java.text.SimpleDateFormat
import scala.util._

import java.text.ParseException

/**
 * Consumer offset type.
 */
object OffsetType extends Enumeration {
  type OffsetType = Value
  val Earliest, Latest, Custom = Value
}

/**
 * Application controller.
 */
class AppController(root: Node) {
  import Settings._
  import ValueType._
  import OffsetType._

  private val log = LoggerFactory.getLogger(getClass)

  private val timeFormatters = List("yyyy-MM-dd'T'HH:mm:ssz", "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mmz", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd") map (p => new SimpleDateFormat(p))

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
    val brokerUrl = node.getConfig("brokerUrl").getString

    log.info(s"Initializing connection node '$name' for '$brokerUrl'")

    node createChild "removeConnection" display "Remove Connection" action removeConnection build ()
    node createChild "publish" display "Publish" action publish build ()

    node createChild "getTopicInfo" display "Get Topic Info" action getTopicInfo build ()
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

    val brokerUrl = topicNode.getParent.getConfig("brokerUrl").getString

    val partition = node.getConfig("partition").getNumber.intValue
    val offsetType = OffsetType withName node.getConfig("offsetType").getString
    val customOffset = node.getConfig("customOffset").getNumber.longValue
    val emitDelay = node.getConfig("emitDelay").getNumber.longValue

    val offset = offsetType match {
      case Earliest => kafka.api.OffsetRequest.EarliestTime
      case Latest   => kafka.api.OffsetRequest.LatestTime
      case _        => customOffset
    }

    val consumer = new MessageConsumer(brokerUrl, topic, partition, offset)
    node.setMetaData(consumer)

    future {
      log.info(s"Streaming started for $topic/$partition with offset $offsetType/$offset")
      consumer.start { (data, currentOffset) =>
        log.debug(s"Message received: ${data.length} bytes")
        node.setValue(anyToValue(new String(data)))
        node.setConfig("offsetType", anyToValue(Custom.toString))
        node.setConfig("customOffset", anyToValue(currentOffset))
        if (emitDelay > 0)
          Thread.sleep(emitDelay)
      }
    }
  }

  /* actions */

  /**
   * Add Connection.
   */
  def addConnection = action(
    parameters = List(
      STRING("name") default "new_connection" description "Connection name",
      STRING("brokers") default DEFAULT_BROKER_URL description "Hosts with optional :port suffix"),
    handler = event => {
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val brokerUrl = event.getParam[String]("brokers", !_.isEmpty, "Brokers cannot be empty").trim
      if (root.children.contains(name))
        throw new IllegalArgumentException(s"Duplicate connection name: $name")

      // throws exception if URL is invalid
      KafkaUtils.parseBrokerList(brokerUrl)

      log.info(s"Adding new connection '$name' for '$brokerUrl'")
      val connNode = root createChild name nodeType "connection" config "brokerUrl" -> anyToValue(brokerUrl) build ()

      initConnNode(connNode)
    })

  /**
   * Remove Connection.
   */
  def removeConnection = (event: ActionResult) => {
    val node = event.getNode.getParent
    val name = node.getName

    val nonEmpty = node.children.values.exists(_.nodeType == Some("topic"))
    if (nonEmpty)
      throw new IllegalArgumentException(s"There are subscriptions for connection '$name'")

    node.delete
    log.info(s"Connection '$name' removed")
  }

  /**
   * Publish.
   */
  def publish = action(
    parameters = List(STRING("topic"), STRING("message")),
    handler = event => {
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

  /**
   * Get Topic Info.
   */
  def getTopicInfo = action(
    parameters = List(STRING("topic"), NUMBER("partition") default 0,
      STRING("time") default timeFormatters(0).format(new java.util.Date)),
    results = List(STRING("errorInfo"), STRING("leader"), NUMBER("firstOffset"), NUMBER("lastOffset"),
      STRING("timeOffset")),
    handler = event => {
      val connNode = event.getNode.getParent
      val brokerUrl = connNode.getConfig("brokerUrl").getString

      val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
      val partitionId = event.getParam[Number]("partition", _.intValue >= 0, "Invalid partition").intValue
      val timeStr = event.getParam[String]("time")

      import KafkaUtils._
      val values = try {
        val brokers = parseBrokerList(brokerUrl)
        val leader = retry(5)(findLeader(brokers, topic, partitionId).map(formatBroker).getOrElse("n/a"))
        val earliest = retry(5)(getEarliestOffset(brokers, topic, partitionId))
        val latest = retry(5)(getLatestOffset(brokers, topic, partitionId))
        val offset = Option(timeStr).filterNot(_.trim.isEmpty).map { str =>
          val time = parseTime(str)
          retry(5)(getOffsetBefore(brokers, topic, partitionId, time.getTime)).map(_.toString) getOrElse "unknown"
        } getOrElse "-"
        List("-", leader, earliest, latest, offset)
      } catch {
        case e: ParseException =>
          log.error("Invalid time format: " + timeStr + ", should be ISO")
          List("Invalid time format", "n/a", null, null, "")
        case e: NumberFormatException =>
          log.error("Invalid broker url: " + brokerUrl)
          List("Invalid broker URL", "n/a", null, null, "")
        case NonFatal(e) =>
          log.error("Error fetching topic information")
          List(getErrorInfo(e), "n/a", null, null, "")
      }

      val row = Row.make(values.map(anyToValue): _*)
      event.getTable.addRow(row)
    })

  /**
   * Subscribe to topic:partition.
   */
  def subscribe = action(
    parameters = List(
      STRING("topic"),
      NUMBER("partition") default 0,
      ENUMS(OffsetType)("offsetType") default "Latest",
      NUMBER("offset") description "Custom offset" default 0,
      NUMBER("emitDelay") description "Delay between two messages" default 0),
    handler = event => {
      val connNode = event.getNode.getParent
      val brokerUrl = connNode.getConfig("brokerUrl").getString

      val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
      val partition = event.getParam[Number]("partition", _.intValue >= 0, "Invalid partition").intValue
      val offsetType = OffsetType withName event.getParam[String]("offsetType")
      val customOffset = event.getParam[Number]("offset").longValue
      val emitDelay = event.getParam[Number]("emitDelay").longValue

      val exists = connNode.children.get(topic).flatMap(_.children.get(s"#$partition")).isDefined
      if (exists)
        throw new IllegalArgumentException(s"Duplicate subscription for topic $topic and partition $partition")

      val topicNode = connNode.children getOrElse (topic, connNode createChild topic nodeType "topic" build ())

      val partitionNode = topicNode createChild s"#$partition" valueType STRING nodeType "partition" config
        ("partition" -> anyToValue(partition), "offsetType" -> anyToValue(offsetType.toString),
          "customOffset" -> anyToValue(customOffset), "emitDelay" -> anyToValue(emitDelay)) build ()

      initPartitionNode(partitionNode)
    })

  /**
   * Unsubscribe.
   */
  def unsubscribe = action(
    parameters = List(NUMBER("partition") default 0),
    handler = (event: ActionResult) => {
      val partition = event.getParam[Number]("partition", _.intValue >= 0, "Invalid partition").intValue

      val topicNode = event.getNode.getParent
      val partitionNode = topicNode.children(s"#$partition")

      val consumer = partitionNode.getMetaData[MessageConsumer]
      if (consumer != null)
        future { consumer.stop } foreach { _ =>
          topicNode.removeChild(partitionNode)
          if (topicNode.children.count(_._2.nodeType == Some("partition")) == 0)
            topicNode.delete

          log.info(s"Subscription ${topicNode.getName}/$partition removed")
        }
    })

  /**
   * Tries to parse a string into a time using one of the available formats.
   */
  @throws(classOf[ParseException])
  def parseTime(str: String) = timeFormatters.foldLeft[Try[java.util.Date]](Failure(null)) { (parsed, fmt) =>
    parsed match {
      case yes @ Success(_) => yes
      case no @ Failure(_)  => Try(fmt.parse(str))
    }
  } get
}