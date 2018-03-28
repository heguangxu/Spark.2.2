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

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class JavaRDD[T](val rdd: RDD[T])(implicit val classTag: ClassTag[T])
  extends AbstractJavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  // Common RDD functions

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
    * 持久化RDD使用默认的存储级别
   */
  def cache(): JavaRDD[T] = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
    * 设置这个RDD的存储级别，在操作第一次计算后持久化它的值。，每个RDD只能调用一次。
   */
  def persist(newLevel: StorageLevel): JavaRDD[T] = wrapRDD(rdd.persist(newLevel))

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * This method blocks until all blocks are deleted.
    * 标志RDD为非持久化的，并从内存和磁盘消除RDD的所有块。这方法阻塞直到所有的块删除。
   */
  def unpersist(): JavaRDD[T] = wrapRDD(rdd.unpersist())

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *  标志RDD为费持久化的，并且从内存和磁盘移除RDD的所有快
   * @param blocking Whether to block until all blocks are deleted.
    *                 是否阻塞直到所有的块删除。
   */
  def unpersist(blocking: Boolean): JavaRDD[T] = wrapRDD(rdd.unpersist(blocking))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * 返回一个新的RDD包含在这个RDD的不同的元素。
   */
  def distinct(): JavaRDD[T] = wrapRDD(rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * 返回一个新的RDD包含在这个RDD的不同的元素。
   */
  def distinct(numPartitions: Int): JavaRDD[T] = wrapRDD(rdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
    * 返回一个新的RDD仅包含满足filter条件的元素。
   */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaRDD[T] =
    wrapRDD(rdd.filter((x => f.call(x).booleanValue())))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
    * 返回一个新的RDD，组成根据指定的numPartitions分区数分区。
   */
  def coalesce(numPartitions: Int): JavaRDD[T] = rdd.coalesce(numPartitions)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
    * 返回一个新的RDD，组成根据指定的numPartitions分区数分区。
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaRDD[T] =
    rdd.coalesce(numPartitions, shuffle)

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *  返回一个新的RDD，恰有numpartitions个分区。
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *  可以增加或减少RDD的并行级别。在内部，这里采用的是一种洗牌重新分配数据。
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
    * 如果你降低这个RDD分区的数量，考虑使用`凝聚``coalesce`,，可以避免进行洗牌。
   */
  def repartition(numPartitions: Int): JavaRDD[T] = rdd.repartition(numPartitions)

  /**
   * Return a sampled subset of this RDD with a random seed.
    * 返回一个采样RDD数据集使用指定的随机种子。
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
    *                        可以元素被多次采样（采样时更换了）
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be greater
   *  than or equal to 0
    *
    * @param fraction 分数预期的样本大小为分数没有更换该RDD的大小：每个元素的选择概率；分数必须[ 0, 1 ]与更换：
    *                 次预期的数量，每个元素被选择；分数必须大于或等于0
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given `RDD`.
   */
  def sample(withReplacement: Boolean, fraction: Double): JavaRDD[T] =
    sample(withReplacement, fraction, Utils.random.nextLong)

  /**
   * Return a sampled subset of this RDD, with a user-supplied seed.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be greater
   *  than or equal to 0
   * @param seed seed for the random number generator
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given `RDD`.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): JavaRDD[T] =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))


  /**
   * Randomly splits this RDD with the provided weights.
    * 根据指定的权重随机分割RDD
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
    *                分裂指定的权重，如果他们总和不为1将会是正常的
   *
   * @return split RDDs in an array  返回一个RDD的分割数组
   */
  def randomSplit(weights: Array[Double]): Array[JavaRDD[T]] =
    randomSplit(weights, Utils.random.nextLong)

  /**
   * Randomly splits this RDD with the provided weights.
    * 根据指定的权重随机分割RDD
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
    *                分裂指定的权重，如果他们总和不为1将会是正常的
   * @param seed random seed  随机种子
   *
   * @return split RDDs in an array 返回一个RDD的分割数组
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[JavaRDD[T]] =
    rdd.randomSplit(weights, seed).map(wrapRDD)

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
    *
    *  并集
   */
  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))


  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
    *
    *   交集
   *
   * @note This method performs a shuffle internally.
   */
  def intersection(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.intersection(other.rdd))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be less than or equal to us.
    *
    * 差集
   */
  def subtract(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaRDD[T], numPartitions: Int): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaRDD[T], p: Partitioner): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, p))

  override def toString: String = rdd.toString

  /** Assign a name to this RDD
    * 为RDD指定一个名称
    * */
  def setName(name: String): JavaRDD[T] = {
    rdd.setName(name)
    this
  }

  /**
   * Return this RDD sorted by the given key function.
    *  根据指定的key函数把RDD进行排序，并且返回
   */
  def sortBy[S](f: JFunction[T, S], ascending: Boolean, numPartitions: Int): JavaRDD[T] = {
    def fn: (T) => S = (x: T) => f.call(x)
    import com.google.common.collect.Ordering  // shadows scala.math.Ordering
    implicit val ordering = Ordering.natural().asInstanceOf[Ordering[S]]
    implicit val ctag: ClassTag[S] = fakeClassTag
    wrapRDD(rdd.sortBy(fn, ascending, numPartitions))
  }

}

object JavaRDD {

  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)

  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}
