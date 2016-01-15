package org.dsa.iot.kafka

import org.dsa.iot.dslink.node.Permission
import org.dsa.iot.dslink.node.actions.{ Action, ActionResult }
import org.dsa.iot.dslink.util.handler.Handler
import org.dsa.iot.dslink.node.value.ValueType

/**
 * DSLink actions.
 */
object Actions {
  import Settings._
  import ValueType._
  import OffsetType._

  /**
   * Add Kafka Connection action.
   */
  def addConnectionAction(controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
        val brokerUrl = event.getParam[String]("brokers", !_.isEmpty, "Brokers cannot be empty").trim
        if (controller.connections.contains(name))
          throw new IllegalArgumentException(s"Duplicate connection name: $name")

        controller.addConnection(name, brokerUrl)
      }
    }).addParameter(STRING("name") default "new_connection" description "Connection name")
      .addParameter(STRING("brokers") default DEFAULT_BROKER_URL description
        "Comma separated list of hosts with optional :port suffix")

  /**
   * Add Subscription action.
   */
  def addSubscriptionAction(connName: String, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
        val partition = event.getParam[Number]("partition", _.intValue >= 0, "Partition cannot be less than zero")
        val subKey = SubscriptionKey(connName, topic, partition.intValue)
        if (controller.subscriptions.contains(subKey))
          throw new IllegalArgumentException(s"Duplicate subscription for topic $topic and partition $partition")
        val offsetType = OffsetType withName event.getParam[String]("offsetType")
        val customOffset = event.getParam[Number]("offset").longValue
        val emitDelay = event.getParam[Number]("emitDelay").longValue

        controller.addSubscription(connName, topic, partition.intValue, offsetType, customOffset, emitDelay)
      }
    }).addParameter(STRING("topic"))
      .addParameter(NUMBER("partition") default 0)
      .addParameter(ENUMS(OffsetType)("offsetType") default "Latest")
      .addParameter(NUMBER("offset") description "Custom offset" default 0)
      .addParameter(NUMBER("emitDelay") description "Delay between two messages" default 0)

  /**
   * Publish Message action.
   */
  def publishAction(brokerUrl: String, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val topic = event.getParam[String]("topic", !_.isEmpty, "Topic cannot be empty").trim
        val message = event.getParam[String]("message", !_.isEmpty, "Message cannot be empty")
        controller.publish(brokerUrl, topic, message)
      }
    }) addParameter STRING("topic") addParameter STRING("message")

  /**
   * Remove Connection action.
   */
  def removeConnectionAction(name: String, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val nonEmpty = controller.subscriptions.exists(_._1.connName == name)
        if (nonEmpty)
          throw new IllegalArgumentException(s"There are active subscriptions for connection $name")

        controller.removeConnection(name)
      }
    })

  /**
   * Remove Subscription action.
   */
  def removeSubscriptionAction(connName: String, topic: String, controller: Controller) =
    new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val partition = event.getParam[Number]("partition", _.intValue >= 0, "Partition cannot be less than zero")
        controller.removeSubscription(connName, topic, partition.intValue)
      }
    }) addParameter (NUMBER("partition") default 0)
}