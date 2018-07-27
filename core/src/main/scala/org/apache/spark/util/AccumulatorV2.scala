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

package org.apache.spark.util

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.{ArrayList, Collections}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{InternalAccumulator, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo

// 累加器的元数据包括id，name（有可能没有），是不是计数失败
private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable


/**
 * The base class for accumulators, that can accumulate inputs of type `IN`, and produce output of
 * type `OUT`.
  * 所有累加器的基类，可以看到这是抽象类！！
 *
 * `OUT` should be a type that can be read atomically (e.g., Int, Long), or thread-safely
 * (e.g., synchronized collections) because it will be read from other threads.
 */
abstract class AccumulatorV2[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _
  // 自动类型推导为Boolean值
  private[this] var atDriverSide = true

  private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    // 注册验证：之前没有被注册过
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues)
    // 将该累加器注册到上下文环境中去
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  /**
   * Returns true if this accumulator has been registered.
   *
   * @note All accumulators must be registered before use, or it will throw exception.
   */
  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.get(metadata.id).isDefined

  private def assertMetadataNotNull(): Unit = {
    if (metadata == null) {
      throw new IllegalStateException("The metadata of this accumulator has not been assigned yet.")
    }
  }

  /**
   * Returns the id of this accumulator, can only be called after registration.
   */
  final def id: Long = {
    assertMetadataNotNull()
    metadata.id
  }

  /**
   * Returns the name of this accumulator, can only be called after registration.
   */
  final def name: Option[String] = {
    assertMetadataNotNull()

    if (atDriverSide) {
      metadata.name.orElse(AccumulatorContext.get(id).flatMap(_.metadata.name))
    } else {
      metadata.name
    }
  }

  /**
   * Whether to accumulate values from failed tasks. This is set to true for system and time
   * metrics like serialization time or bytes spilled, and false for things with absolute values
   * like number of input rows.  This should be used for internal metrics only.
   */
  private[spark] final def countFailedValues: Boolean = {
    assertMetadataNotNull()
    metadata.countFailedValues
  }

  /**
   * Creates an [[AccumulableInfo]] representation of this [[AccumulatorV2]] with the provided
   * values.
   */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  /**
   * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
   * value; for a list accumulator, Nil is zero value.
   */
  def isZero: Boolean

  /**
   * Creates a new copy of this accumulator, which is zero value. i.e. call `isZero` on the copy
   * must return true.
   */
  def copyAndReset(): AccumulatorV2[IN, OUT] = {
    val copyAcc = copy()
    copyAcc.reset()
    copyAcc
  }

  /**
   * Creates a new copy of this accumulator.
   */
  def copy(): AccumulatorV2[IN, OUT]

  /**
   * Resets this accumulator, which is zero value. i.e. call `isZero` must
   * return true.
   */
  def reset(): Unit

  /**
   * Takes the inputs and accumulates.
   */
  def add(v: IN): Unit

  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place.
   */
  def merge(other: AccumulatorV2[IN, OUT]): Unit

  /**
   * Defines the current value of this accumulator
   */
  def value: OUT

  // Called by Java when serializing an object
  final protected def writeReplace(): Any = {
    if (atDriverSide) {
      if (!isRegistered) {
        throw new UnsupportedOperationException(
          "Accumulator must be registered before send to executor")
      }
      val copyAcc = copyAndReset()
      assert(copyAcc.isZero, "copyAndReset must return a zero value copy")
      val isInternalAcc = name.isDefined && name.get.startsWith(InternalAccumulator.METRICS_PREFIX)
      if (isInternalAcc) {
        // Do not serialize the name of internal accumulator and send it to executor.
        copyAcc.metadata = metadata.copy(name = None)
      } else {
        // For non-internal accumulators, we still need to send the name because users may need to
        // access the accumulator name at executor side, or they may keep the accumulators sent from
        // executors and access the name when the registered accumulator is already garbage
        // collected(e.g. SQLMetrics).
        copyAcc.metadata = metadata
      }
      copyAcc
    } else {
      this
    }
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    if (atDriverSide) {
      atDriverSide = false

      // Automatically register the accumulator when it is deserialized with the task closure.
      // This is for external accumulators and internal ones that do not represent task level
      // metrics, e.g. internal SQL metrics, which are per-operator.
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        taskContext.registerAccumulator(this)
      }
    } else {
      atDriverSide = true
    }
  }

  override def toString: String = {
    if (metadata == null) {
      "Un-registered Accumulator: " + getClass.getSimpleName
    } else {
      getClass.getSimpleName + s"(id: $id, name: $name, value: $value)"
    }
  }
}


/**
 * An internal class used to track accumulators by Spark itself.
  * 累加器的上下文环境,这是一个单例类！！全局只有一个
 */
private[spark] object AccumulatorContext extends Logging {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
    // 引用是java的概念，在java.lang.ref包里，弱引用比较适合用来管理内存特别大的高速缓存
    // 弱引用引用的是AccumulatorV2累加器，一旦引用累加器的RDD和用户代码被清理了，累加器就有可能被垃圾回收
    // 这是一个线程安全的容器
    // 键值是累计器的id
    // 这个map就是该最核心的容器，管理着所有的累加器
  private val originals = new ConcurrentHashMap[Long, jl.ref.WeakReference[AccumulatorV2[_, _]]]

  // 原子Long
  private[this] val nextId = new AtomicLong(0L)

  /**
   * Returns a globally unique ID for a new [[AccumulatorV2]].
   * Note: Once you copy the [[AccumulatorV2]] the ID is no longer unique.
    * 如果拷贝累加器，里面的id也会被拷贝，这个时候id就不唯一了
   */
  // 对于其他线程的请求，都返回一个新的唯一的累加器id
  def newId(): Long = nextId.getAndIncrement

  /** Returns the number of accumulators registered. Used in testing. */
  // 有多少累加器
  def numAccums: Int = originals.size

  /**
   * Registers an [[AccumulatorV2]] created on the driver such that it can be used on the executors.
    * 在驱动器注册累加器，在执行器使用累加器
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what `org.apache.spark.scheduler.DAGScheduler` does.
    * DAG用这里注册的累加器作为一个容器，累计不同任务的值
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
    *
   *
   * If an [[AccumulatorV2]] with the same ID was already registered, this does nothing instead
   * of overwriting it. We will never register same accumulator twice, this is just a sanity check.
    * 如果已经注册了就不注册了
   */
  def register(a: AccumulatorV2[_, _]): Unit = {
    originals.putIfAbsent(a.id, new jl.ref.WeakReference[AccumulatorV2[_, _]](a))
  }

  /**
   * Unregisters the [[AccumulatorV2]] with the given ID, if any.
   */
  def remove(id: Long): Unit = {
    originals.remove(id)
  }

  /**
   * Returns the [[AccumulatorV2]] registered with the given ID, if any.
   */
  def get(id: Long): Option[AccumulatorV2[_, _]] = {
    val ref = originals.get(id)
    if (ref eq null) {
      None
    } else {
      // Since we are storing weak references, warn when the underlying data is not valid.
      // 使用弱引用，需要首先判断，该引用还存在么？
      val acc = ref.get
      if (acc eq null) {
        logWarning(s"Attempted to access garbage collected accumulator $id")
      }
      Option(acc)
    }
  }

  /**
   * Clears all registered [[AccumulatorV2]]s. For testing only.
   */
  def clear(): Unit = {
    originals.clear()
  }

  // Identifier for distinguishing SQL metrics from other accumulators
  private[spark] val SQL_ACCUM_IDENTIFIER = "sql"
}


/**
 * An [[AccumulatorV2 accumulator]] for computing sum, count, and average of 64-bit integers.
 *
 * @since 2.0.0
 */
class LongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  // 所以LongAccumulator里面其实没啥，就两个Long字段，一个_sum，一个_count
  private var _sum = 0L
  private var _count = 0L

  /**
   * Returns false if this accumulator has had any values added to it or the sum is non-zero.
   *
   * @since 2.0.0
   */
  override def isZero: Boolean = _sum == 0L && _count == 0

  override def copy(): LongAccumulator = {
    val newAcc = new LongAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  // 两个字段都重置为0
  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  // _sum添加具体值，计数加1
  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  // 一个方法设置了两个参数，一个是java的Long，一个是scala的Long，设置了版本的兼容性
  def add(v: Long): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Returns the number of elements added to the accumulator.
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * Returns the sum of elements added to the accumulator.
   * @since 2.0.0
   */
  def sum: Long = _sum

  /**
   * Returns the average of elements added to the accumulator.
   * @since 2.0.0
   */
  def avg: Double = _sum.toDouble / _count

  // merge其实就是定义两个AccumulatorV2在一起的运算方式，其实就是简单的sum相加，count相加
  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  // 取值函数其实就是取的sum值
  override def value: jl.Long = _sum
}


/**
 * An [[AccumulatorV2 accumulator]] for computing sum, count, and averages for double precision
 * floating numbers.
 *
 * @since 2.0.0
 */
// Double和Long没啥区别。。。就里面的_sum变量变了
class DoubleAccumulator extends AccumulatorV2[jl.Double, jl.Double] {
  private var _sum = 0.0
  private var _count = 0L

  /**
   * Returns false if this accumulator has had any values added to it or the sum is non-zero.
   */
  override def isZero: Boolean = _sum == 0.0 && _count == 0

  override def copy(): DoubleAccumulator = {
    val newAcc = new DoubleAccumulator
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0.0
    _count = 0L
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Adds v to the accumulator, i.e. increment sum by v and count by 1.
   * @since 2.0.0
   */
  def add(v: Double): Unit = {
    _sum += v
    _count += 1
  }

  /**
   * Returns the number of elements added to the accumulator.
   * @since 2.0.0
   */
  def count: Long = _count

  /**
   * Returns the sum of elements added to the accumulator.
   * @since 2.0.0
   */
  def sum: Double = _sum

  /**
   * Returns the average of elements added to the accumulator.
   * @since 2.0.0
   */
  def avg: Double = _sum / _count

  override def merge(other: AccumulatorV2[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def value: jl.Double = _sum
}


/**
 * An [[AccumulatorV2 accumulator]] for collecting a list of elements.
 *
 * @since 2.0.0
 */
class CollectionAccumulator[T] extends AccumulatorV2[T, java.util.List[T]] {
  // java中的Collections.synchronizedList类，是一个同步类，使用的时候规定，必须以该同步list对象为锁，进行遍历！！
  // 为啥用同步list？因为会有多个线程，比如多个executor访问这个集合啊
  private val _list: java.util.List[T] = Collections.synchronizedList(new ArrayList[T]())

  /**
   * Returns false if this accumulator instance has any values in it.
   */
  override def isZero: Boolean = _list.isEmpty

  override def copyAndReset(): CollectionAccumulator[T] = new CollectionAccumulator

  override def copy(): CollectionAccumulator[T] = {
    val newAcc = new CollectionAccumulator[T]
    // 可以看到对同步list的访问必须以该对象为锁
    _list.synchronized {
      newAcc._list.addAll(_list)
    }
    newAcc
  }

  override def reset(): Unit = _list.clear()

  // 集合累加器的添加就是添加元素了
  override def add(v: T): Unit = _list.add(v)

  override def merge(other: AccumulatorV2[T, java.util.List[T]]): Unit = other match {
    case o: CollectionAccumulator[T] => _list.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = _list.synchronized {
    // unmodifiableList是返回一个List的View，即只读的集合，任何写都会触发错误
    // 以为我们要返回这个value！！所以我们预见不了别人会怎么处理这个集合！！才这样做的
    java.util.Collections.unmodifiableList(new ArrayList[T](_list))
  }

  private[spark] def setValue(newValue: java.util.List[T]): Unit = {
    _list.clear()
    _list.addAll(newValue)
  }
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends AccumulatorV2[T, R] {
  private[spark] var _value = initialValue  // Current value on driver

  @transient private lazy val _zero = param.zero(initialValue)

  override def isZero: Boolean = _value.asInstanceOf[AnyRef].eq(_zero.asInstanceOf[AnyRef])

  override def copy(): LegacyAccumulatorWrapper[R, T] = {
    val acc = new LegacyAccumulatorWrapper(initialValue, param)
    acc._value = _value
    acc
  }

  override def reset(): Unit = {
    _value = _zero
  }

  override def add(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def merge(other: AccumulatorV2[T, R]): Unit = other match {
    case o: LegacyAccumulatorWrapper[R, T] => _value = param.addInPlace(_value, o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: R = _value
}
