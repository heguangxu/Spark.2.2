/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.api.java

import java.{util => ju}
import java.util.Map.Entry

import scala.collection.mutable

private[spark] object JavaUtils {

  // 判断这个option可选的值是否定义了，如果定义了就返回，否则就返回empty对象
  def optionToOptional[T](option: Option[T]): Optional[T] =
    if (option.isDefined) {
      Optional.of(option.get)
    } else {
      Optional.empty[T]
    }

  // Workaround for SPARK-3926 / SI-8911
  def mapAsSerializableJavaMap[A, B](underlying: collection.Map[A, B]): SerializableMapWrapper[A, B]
  = new SerializableMapWrapper(underlying)

  // Implementation is copied from scala.collection.convert.Wrappers.MapWrapper,
  // but implements java.io.Serializable. It can't just be subclassed to make it
  // Serializable since the MapWrapper class has no no-arg constructor. This class
  // doesn't need a no-arg constructor though.

  // 实现从scala.collection.convert.wrappers.mapwrapper复制，而是实现java.io.serializable。
  // 它不能被继承使其序列化自mapwrapper类没有无参数构造函数。这类不需要一个无参数构造函数虽然。

  class SerializableMapWrapper[A, B](underlying: collection.Map[A, B])
    extends ju.AbstractMap[A, B] with java.io.Serializable { self =>

    override def size: Int = underlying.size

    override def get(key: AnyRef): B = try {
      underlying.getOrElse(key.asInstanceOf[A], null.asInstanceOf[B])
    } catch {
      case ex: ClassCastException => null.asInstanceOf[B]
    }

    override def entrySet: ju.Set[ju.Map.Entry[A, B]] = new ju.AbstractSet[ju.Map.Entry[A, B]] {
      override def size: Int = self.size

      override def iterator: ju.Iterator[ju.Map.Entry[A, B]] = new ju.Iterator[ju.Map.Entry[A, B]] {
        val ui = underlying.iterator
        var prev : Option[A] = None

        def hasNext: Boolean = ui.hasNext

        def next(): Entry[A, B] = {
          val (k, v) = ui.next()
          prev = Some(k)
          new ju.Map.Entry[A, B] {
            import scala.util.hashing.byteswap32
            override def getKey: A = k
            override def getValue: B = v
            override def setValue(v1 : B): B = self.put(k, v1)
            override def hashCode: Int = byteswap32(k.hashCode) + (byteswap32(v.hashCode) << 16)
            override def equals(other: Any): Boolean = other match {
              case e: ju.Map.Entry[_, _] => k == e.getKey && v == e.getValue
              case _ => false
            }
          }
        }

        def remove() {
          prev match {
            case Some(k) =>
              underlying match {
                case mm: mutable.Map[A, _] =>
                  mm.remove(k)
                  prev = None
                case _ =>
                  throw new UnsupportedOperationException("remove")
              }
            case _ =>
              throw new IllegalStateException("next must be called at least once before remove")
          }
        }
      }
    }
  }
}

/**
Scala Option(选项)类型用来表示一个值是可选的（有值或无值)。
  Option[T] 是一个类型为 T 的可选值的容器： 如果值存在， Option[T] 就是一个 Some[T] ，如果不存在， Option[T] 就是对象 None 。
  接下来我们来看一段代码：
  // 虽然 Scala 可以不定义变量的类型，不过为了清楚些，我还是
  // 把他显示的定义上了

  val myMap: Map[String, String] = Map("key1" -> "value")
  val value1: Option[String] = myMap.get("key1")
  val value2: Option[String] = myMap.get("key2")

  println(value1) // Some("value1")
  println(value2) // None
  在上面的代码中，myMap 一个是一个 Key 的类型是 String，Value 的类型是 String 的 hash map，但不一样的是他的 get() 返回的是一个叫 Option[String] 的类别。
  Scala 使用 Option[String] 来告诉你：「我会想办法回传一个 String，但也可能没有 String 给你」。
  myMap 里并没有 key2 这笔数据，get() 方法返回 None。
  Option 有两个子类别，一个是 Some，一个是 None，当他回传 Some 的时候，代表这个函式成功地给了你一个 String，而你可以透过 get() 这个函式拿到那个 String，如果他返回的是 None，则代表没有字符串可以给你。
  */