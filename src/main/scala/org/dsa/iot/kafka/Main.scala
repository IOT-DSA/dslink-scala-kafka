package org.dsa.iot.kafka

import org.dsa.iot.dslink.{ DSLink, DSLinkFactory, DSLinkHandler }
import org.slf4j.LoggerFactory

/**
 * Kafka DSLink.
 */
object Main extends DSLinkHandler {

  private val log = LoggerFactory.getLogger(getClass)

  override val isResponder = true

  override def onResponderInitialized(link: DSLink) = {
    log.info("Responder initialized")
    new AppController(link.getNodeManager.getSuperRoot)
  }

  /**
   * DSLink entry point.
   */
  def main(args: Array[String]): Unit = DSLinkFactory.start(args, this)
}