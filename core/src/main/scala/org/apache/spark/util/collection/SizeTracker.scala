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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /** Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
  private val samples = new mutable.Queue[Sample]

  /** The average number of bytes per update between our last two samples. */
  private var bytesPerUpdate: Double = _

  /** Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _

  /** The value of 'numUpdates' at which we will take our next sample. */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /**
   * Callback to be invoked after every update.
    *
    *   AppendOnlyMap的容量增长实现方法growTable，那是不是意味着AppendOnlyMap的容量可以无限制的增长呢？当然不是，我们需要
    * 对AppendOnlyMap大小进行限制。很明显我们可以再AppendOnlyMap的每次更新操作之后计算它的大小，这应该没有什么问题。
    * Spark为大数据平台需要提供实时计算能力，无论是数据量还是对CPU的开销都很大。每当发生update或者insert操作就计算一次大小
    * 会严重影响Spark的性能，因此Sparks实际上采用了采样并且利用这些采样对AppendOnlyMap未来的大小进行评估或者推测的方式。
    *
    *   SizeTrackingAppendOnlyMap继承了特质SizeTracker,其中afterUpdate用于每次更新AppendOnlyMap的缓存后进行采样，采样前提
    * 是已经达到设定的采样间隔（nextSampleNum），其中处理步骤如下：
    *   1.将AppendOnlyMap所占用的内存进行评估并且与当前编号（numUpdates）一起作为样本跟信道samples = new mutable.Queue[Sample]中。
    *   2.如果当前采样数量大于2，则使samples执行一次出队操作，保证样本总数等于2.
    *   3.计算每次更新增加的大小，公式如下：
    *       bytesPerUpdate = (本兮采集大小 - 上次采样大小) / (本次采集编号 - 上次采样编号)
    *     如果样本数小于2，那么bytesPerUpdate =  0;
    *   4,计算下次采样的间隔nextSampleNum.
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   */
  /**
    * Callback to be invoked after every update.
    *
    *   AppendOnlyMap的容量增长实现方法growTable，那是不是意味着AppendOnlyMap的容量可以无限制的增长呢？当然不是，我们需要
    * 对AppendOnlyMap大小进行限制。很明显我们可以再AppendOnlyMap的每次更新操作之后计算它的大小，这应该没有什么问题。
    * Spark为大数据平台需要提供实时计算能力，无论是数据量还是对CPU的开销都很大。每当发生update或者insert操作就计算一次大小
    * 会严重影响Spark的性能，因此Sparks实际上采用了采样并且利用这些采样对AppendOnlyMap未来的大小进行评估或者推测的方式。
    *
    *   SizeTrackingAppendOnlyMap继承了特质SizeTracker,其中afterUpdate用于每次更新AppendOnlyMap的缓存后进行采样，采样前提
    * 是已经达到设定的采样间隔（nextSampleNum），其中处理步骤如下：
    *   1.将AppendOnlyMap所占用的内存进行评估并且与当前编号（numUpdates）一起作为样本跟信道samples = new mutable.Queue[Sample]中。
    *   2.如果当前采样数量大于2，则使samples执行一次出队操作，保证样本总数等于2.
    *   3.计算每次更新增加的大小，公式如下：
    *       bytesPerUpdate = (本兮采集大小 - 上次采样大小) / (本次采集编号 - 上次采样编号)
    *     如果样本数小于2，那么bytesPerUpdate =  0;
    *   4,计算下次采样的间隔nextSampleNum.
    */
  private def takeSample(): Unit = {
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    bytesPerUpdate = math.max(0, bytesDelta)
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
    * AppendOnlyMap的大小采样数据用于推测AppendOnlyMap未来的大小，推测的实现如下：
    * 由于SizeTrackingPairBuffer也继承了SizeTracker，所以estimateSize方法不但对AppendOnlyMap也对SizeTrackingPairBuffer在
    * 内存中的容量进行限制以防止内存溢出时发挥其作用。
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
