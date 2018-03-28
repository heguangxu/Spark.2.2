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

package org.apache.spark

/**
  * Spark's broadcast variables, used to broadcast immutable datasets to all nodes.
  */
package object broadcast {
  // For package docs only
}
//  Spark的广播变量，用于广播不可变的数据集到所有节点

/*
  详情查看：http://blog.csdn.net/qq_21383435/article/details/78181716

  Scala 2.8提供包对象（package object）的新特性。什么是包对象呢？按我的理解，根据Scala“一切皆对象”设计哲学，
  包(package)也是一种对象。既然是对象，那么就应该有属性和方法，也可以在包对象内声明某个类型的别名。

  我们可以像使用伴随对象那样使用包对象

  如果包对象的作用仅仅限于伴随对象那样，那scala 2.8完全没有必要引入这种特性。实际上包对象最重要的用途是兼容旧的类库，或者为某些数据类型提供增强版本。

  比如在scala 2.7.7中，List是定义在scala包下的一个不可变集合类。这样做的目的是每次使用List的时候不需要显式地导入包名，因为List是一个使用很频繁的类。

  在Scala中，包java.lang，包scala和伴随对象Predef里的所有数据类型，属性和方法会被自动导入到每个Scala文件中
  然而另一方面，List由于具有不可变的特性，它应该归入scala.collection.immutable包中。因此在scala 2.8中，List被挪到
  scala.collection.immutable包下里，但这样一来2.8版本如何兼容2.7.7版本。于是Scala 2.8引入“包对象”这个特性来解决这个问题。
  如果你查阅Scala 2.8版本的API文档或源码，你会发现它定义了一个包对象:

  package object scala {

      // Type and value aliases for collection classes

      val List = scala.collection.immutable.List

       //  others

   }


   也就是说scala.List相当于指向了scala.collection.immutable.List，从而保证升级到Scala 2.8版本不会对原有的程序造成影响。
 */