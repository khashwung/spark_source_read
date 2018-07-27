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

package org.apache.spark.executor

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util._


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
  * Metrics在软件开发中的语义就是资源性能监控指标
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
  * 该类就是若干个内部accumulators的wrapper，累计器就是一个线程安全的类，用来统计信息
  * 累加器的所用原理是：当task完成，执行器会向驱动器发送一个局部值，并更新累加器，所有的在驱动器上
  * 注册过的执行器都这么做，所以这是并行执行的。
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
  * 累加器也会随着心跳周期性的更新
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
  // Each metric is internally represented as an accumulator
  // 可以看到所有的内部存储状态都是Accumulator类型
  private val _executorDeserializeTime = new LongAccumulator    // 执行器的反序列化时间
  private val _executorDeserializeCpuTime = new LongAccumulator // 执行器反序列化花费的cpu时间
  private val _executorRunTime = new LongAccumulator            // 执行器的运行时间
  private val _executorCpuTime = new LongAccumulator            // 执行器的cpu时间
  private val _resultSize = new LongAccumulator                 // 计算结果的大小？？
  private val _jvmGCTime = new LongAccumulator                  // jvm的gc时间
  private val _resultSerializationTime = new LongAccumulator    // 结果的序列化时间
  private val _memoryBytesSpilled = new LongAccumulator         // 任务产生出的放到内存里的存储大小
  private val _diskBytesSpilled = new LongAccumulator           // 就是内存呆不了了，只能持久化到磁盘里的大小
  private val _peakExecutionMemory = new LongAccumulator        // 执行器内存的峰值达到多少
  private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
    * 里面有sum值，也有计数值，sum值就是我们想要的值
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   *
   * Tracking the _updatedBlockStatuses can use a lot of memory.
   * It is not used anywhere inside of Spark so we would ideally remove it, but its exposed to
   * the user in SparkListenerTaskEnd so the api is kept for compatibility.
   * Tracking can be turned off to save memory via config
   * TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get("spark.testing").map(_ => new LongAccumulator)


  import InternalAccumulator._
  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.REMOTE_BYTES_READ_TO_DISK -> shuffleReadMetrics._remoteBytesReadToDisk,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums

  private[spark] def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]] = {
    // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
    // value will be updated at driver side.
    internalAccums.filter(a => !a.isZero || a == _resultSize)
  }
}


private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    tm
  }
}