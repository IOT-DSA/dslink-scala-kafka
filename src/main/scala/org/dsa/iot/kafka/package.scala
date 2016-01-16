package org.dsa.iot

import scala.collection.JavaConverters._

import org.dsa.iot.dslink.node.{ Node, NodeBuilder, Permission, Writable }
import org.dsa.iot.dslink.node.actions.{ Action, ActionResult, EditorType, Parameter, ResultType }
import org.dsa.iot.dslink.node.value.{ Value, ValueType }
import org.dsa.iot.dslink.util.handler.Handler
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

/**
 * Helper functions and types for Kafka DSLink.
 */
package object kafka {

  type ActionHandler = ActionResult => Unit

  /* Action */

  def action(handler: ActionHandler,
             parameters: Iterable[Parameter] = Nil,
             results: Iterable[Parameter] = Nil,
             permission: Permission = Permission.READ,
             resultType: ResultType = ResultType.VALUES): Action = {
    val a = new Action(permission, new Handler[ActionResult] {
      def handle(event: ActionResult) = handler(event)
    })
    parameters foreach a.addParameter
    results foreach a.addResult
    a.setResultType(resultType)
    a
  }

  /* ActionResult */

  implicit def stringExtractor(v: Value) = v.getString
  implicit def numberExtractor(v: Value) = v.getNumber

  implicit class RichActionResult(val event: ActionResult) extends AnyVal {
    def getParam[T](name: String, check: T => Boolean = (_: T) => true, msg: String = "")(implicit ex: Value => T): T = {
      val value = ex(event.getParameter(name))

      if (!check(value))
        throw new IllegalArgumentException(msg)
      else
        value
    }
  }

  /* ValueType */

  def ENUMS(enum: Enumeration) = ValueType.makeEnum(enum.values.map(_.toString).asJava)

  implicit class RichValueType(val vt: ValueType) extends AnyVal {
    def apply(name: String) = new Parameter(name, vt)
  }

  /* Parameter */

  implicit class RichParameter(val param: Parameter) extends AnyVal {
    def default(value: Any) = param having (_.setDefaultValue(anyToValue(value)))
    def description(value: String) = param having (_.setDescription(value))
    def editorType(value: EditorType) = param having (_.setEditorType(value))
    def placeHolder(value: String) = param having (_.setPlaceHolder(value))
    def meta(value: JsonObject) = param having (_.setMetaData(value))
  }

  /* Node */

  implicit class RichNode(val node: Node) extends AnyVal {
    def nodeType = Option(node.getConfig("nodeType")) map (_.getString)
    def children = node.getChildren.asScala.toMap
  }

  /* NodeBuilder */

  /**
   * Pimps up NodeBuilder by providing Scala fluent syntax.
   */
  implicit class RichNodeBuilder(val nb: NodeBuilder) extends AnyVal {

    def display(name: String) = nb having (_.setDisplayName(name))

    def attributes(tpls: (String, Value)*) = {
      tpls foreach (t => nb.setAttribute(t._1, t._2))
      nb
    }

    def config(configs: (String, Value)*) = {
      configs foreach (c => nb.setConfig(c._1, c._2))
      nb
    }

    def roConfig(configs: (String, Value)*) = {
      configs foreach (c => nb.setRoConfig(c._1, c._2))
      nb
    }

    def nodeType(nType: String) = nb having (_.setConfig("nodeType", anyToValue(nType)))

    def valueType(vType: ValueType) = nb having (_.setValueType(vType))

    def value(value: Value) = nb having (_.setValue(value))

    def hidden(flag: Boolean) = nb having (_.setHidden(flag))

    def profile(p: String) = nb having (_.setProfile(p))

    def meta(md: Any) = nb having (_.setMetaData(md))

    def serializable(flag: Boolean) = nb having (_.setSerializable(flag))

    def writable(w: Writable) = nb having (_.setWritable(w))

    def action(action: Action): NodeBuilder = nb having (_.setAction(action))

    def action(handler: ActionHandler, permission: Permission = Permission.READ): NodeBuilder =
      action(new Action(permission, new Handler[ActionResult] {
        def handle(event: ActionResult) = handler(event)
      }))
  }

  /**
   * Extracts the data from a Value object.
   */
  def valueToAny(value: Value): Any = value.getType.toJsonString match {
    case ValueType.JSON_BOOL   => value.getBool
    case ValueType.JSON_NUMBER => value.getNumber
    case ValueType.JSON_MAP    => jsonObjectToMap(value.getMap)
    case ValueType.JSON_ARRAY  => jsonArrayToList(value.getArray)
    case _                     => value.getString
  }

  /**
   * Converts a JsonArray instance into a scala List[Any].
   */
  def jsonArrayToList(arr: JsonArray): List[Any] = arr.getList.asScala.toList map {
    case x: JsonArray  => jsonArrayToList(x)
    case x: JsonObject => jsonObjectToMap(x)
    case x             => x
  }

  /**
   * Converts a JsonObject instance into a scala Map[String, Any].
   */
  def jsonObjectToMap(obj: JsonObject): Map[String, Any] = obj.getMap.asScala.toMap mapValues {
    case x: JsonArray  => jsonArrayToList(x)
    case x: JsonObject => jsonObjectToMap(x)
    case x             => x
  }

  /**
   * Converts a value into Value object.
   */
  def anyToValue(value: Any): Value = value match {
    case null                => null
    case x: java.lang.Number => new Value(x)
    case x: Boolean          => new Value(x)
    case x: String           => new Value(x)
    case x: Map[_, _]        => new Value(mapToJsonObject(x.asInstanceOf[Map[String, _]]))
    case x: List[_]          => new Value(listToJsonArray(x))
    case x @ _               => new Value(x.toString)
  }

  /**
   * Converts a scala List[Any] instance into a JsonArray.
   */
  def listToJsonArray(ls: List[_]): JsonArray = {
    val elements = ls map {
      case x: List[_]   => listToJsonArray(x)
      case x: Map[_, _] => mapToJsonObject(x.asInstanceOf[Map[String, Any]])
      case x            => x
    }
    new JsonArray(elements.asJava)
  }

  /**
   * Converts a scala Map[String, Any] instance into a JsonObject.
   */
  def mapToJsonObject(mp: Map[String, _]): JsonObject = {
    val elements = mp.mapValues {
      case x: List[_]   => listToJsonArray(x)
      case x: Map[_, _] => mapToJsonObject(x.asInstanceOf[Map[String, Any]])
      case x            => x.asInstanceOf[Object]
    }
    // due to issues with mutability, have to do it the log way instead of elements.toJava
    val m = new java.util.HashMap[String, Object]
    elements foreach {
      case (key, value) => m.put(key, value)
    }
    new JsonObject(m)
  }

  /**
   * Helper class providing a simple syntax to add side effects to the returned value:
   *
   * {{{
   * def square(x: Int) = {
   *            x * x
   * } having (r => println "returned: " + r)
   * }}}
   *
   * or simplified
   *
   * {{{
   * def square(x: Int) = (x * x) having println
   * }}}
   */
  final implicit class Having[A](val result: A) extends AnyVal {
    def having(body: A => Unit): A = {
      body(result)
      result
    }
    def having(body: => Unit): A = {
      body
      result
    }
  }
}