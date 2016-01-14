package org.dsa.iot.kafka

import scala.collection.JavaConverters.{ mapAsScalaMapConverter, seqAsJavaListConverter }

import org.dsa.iot.dslink.node.{ Node, Permission }
import org.dsa.iot.dslink.node.actions.{ Action, ActionResult }
import org.dsa.iot.dslink.util.handler.Handler
import org.dsa.iot.dslink.node.value.ValueType

import kafka.common.TopicAndPartition

/**
 * DSLink actions.
 */
object Actions {
  import Settings._
  import ValueType._

  /**
   * Add Kafka Connection action.
   */
  def addConnectionAction(parent: Node, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
        val brokerUrl = event.getParam[String]("brokers", !_.isEmpty, "Brokers cannot be empty").trim
        if (parent.getChildren.asScala.exists(_._1 == name))
          throw new IllegalArgumentException(s"Duplicate connection name: $name")

        controller.addConnection(parent, name, brokerUrl)
      }
    }).addParameter(STRING("name") default "new_connection" description "Connection name")
      .addParameter(STRING("brokers") default DEFAULT_BROKER_URL description
        "Comma separated list of hosts with optional :port suffix")

  /**
   * Add Subscription action.
   */
  def addSubscriptionAction(parent: Node, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty")
        val partition = event.getParam[Number]("partition", _.intValue >= 0, "Partition cannot be less than zero")
        val tap = TopicAndPartition(topic, partition.intValue)
        val consumers = controller.consumers.getOrElse(parent.getName, Map.empty[TopicAndPartition, MessageConsumer])
        if (consumers.contains(tap))
          throw new IllegalArgumentException(s"Duplicate topic $topic and partition $partition")
        val offsetType = event.getParam[String]("offsetType")
        val offset = event.getParam[Number]("offset").longValue

        controller.addSubcription(parent, topic, partition.intValue, offsetType, offset)
      }
    }) addParameter
      STRING("topic") addParameter (NUMBER("partition") default 0) addParameter
      (makeEnum(Seq("Earliest", "Latest", "Custom").asJava)("offsetType") default "Latest") addParameter
      (NUMBER("offset") description "Custom offset" default 0)

  /**
   * Publish Message action.
   */
  def publishAction(parent: Node, controller: Controller) = new Action(Permission.READ, new Handler[ActionResult] {
    def handle(event: ActionResult) = {
      val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty")
      val message = event.getParam[String]("message", !_.isEmpty, "Message cannot be empty")
      controller.publish(parent.getAttribute("brokerUrl"), topic, message)
    }
  }) addParameter STRING("topic") addParameter STRING("message")

  /**
   * Remove Connection action.
   */
  def removeConnectionAction(node: Node, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = controller.removeConnection(node)
    })

  /**
   * Remove Subscription action.
   */
  def removeSubscriptionAction(node: Node, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val connName = node.getParent.getName
        val topic = node.getName
        val partition = event.getParam[Number]("partition", _.intValue >= 0, "Partition cannot be less than zero")
        val consumers = controller.consumers.getOrElse(connName, Map.empty[TopicAndPartition, MessageConsumer])
        val tap = TopicAndPartition(topic, partition.intValue)
        consumers.get(tap).foreach(_.stop)
        node.removeChild(s"#$partition")
      }
    }) addParameter (NUMBER("partition") default 0)
}