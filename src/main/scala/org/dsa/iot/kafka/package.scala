package org.dsa.iot

import scala.collection.JavaConverters._

import org.dsa.iot.dslink.node.actions.{ ActionResult, EditorType, Parameter }
import org.dsa.iot.dslink.node.value.{ Value, ValueType }
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

package object kafka {

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

  implicit class RichValueType(val vt: ValueType) extends AnyVal {
    def apply(name: String) = new Parameter(name, vt)
  }

  implicit class RichParameter(val param: Parameter) extends AnyVal {
    def default(value: Any) = param having (_.setDefaultValue(anyToValue(value)))
    def description(value: String) = param having (_.setDescription(value))
    def editorType(value: EditorType) = param having (_.setEditorType(value))
    def placeHolder(value: String) = param having (_.setPlaceHolder(value))
    def meta(value: JsonObject) = param having (_.setMetaData(value))
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