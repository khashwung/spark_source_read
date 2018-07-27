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

package org.apache.spark.status

import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashMap, ListBuffer}

import com.google.common.util.concurrent.MoreExecutors

import org.apache.spark.SparkConf
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.kvstore._

/**
 * A KVStore wrapper that allows tracking the number of elements of specific types, and triggering
 * actions once they reach a threshold. This allows writers, for example, to control how much data
 * is stored by potentially deleting old data as new data is added.
 *
 * This store is used when populating data either from a live UI or an event log. On top of firing
 * triggers when elements reach a certain threshold, it provides two extra bits of functionality:
  * 也是KVStore的包装类，当存储到达一定的阈值，会触发triggers
 *
 * - a generic worker thread that can be used to run expensive tasks asynchronously; the tasks can
 *   be configured to run on the calling thread when more determinism is desired (e.g. unit tests).
 * - a generic flush mechanism so that listeners can be notified about when they should flush
 *   internal state to the store (e.g. after the SHS finishes parsing an event log).
 *
 * The configured triggers are run on a separate thread by default; they can be forced to run on
 * the calling thread by setting the `ASYNC_TRACKING_ENABLED` configuration to `false`.
 */
private[spark] class ElementTrackingStore(store: KVStore, conf: SparkConf) extends KVStore {

  // 经常卸载里面，只给这个类用
  import config._

  private val triggers = new HashMap[Class[_], Seq[Trigger[_]]]()
  private val flushTriggers = new ListBuffer[() => Unit]()
  private val executor = if (conf.get(ASYNC_TRACKING_ENABLED)) {
    // 如果异步，就创建新的线程
    ThreadUtils.newDaemonSingleThreadExecutor("element-tracking-store-worker")
  } else {
    MoreExecutors.sameThreadExecutor()
  }

  // volatile控制可见性
  @volatile private var stopped = false

  /**
   * Register a trigger that will be fired once the number of elements of a given type reaches
   * the given threshold.
    * 添加新的trigger，trigger的作用就是状态满足一定条件，立马执行操作
   *
   * @param klass The type to monitor.
   * @param threshold The number of elements that should trigger the action.
   * @param action Action to run when the threshold is reached; takes as a parameter the number
   *               of elements of the registered type currently known to be in the store.
   */
  def addTrigger(klass: Class[_], threshold: Long)(action: Long => Unit): Unit = {
    val existing = triggers.getOrElse(klass, Seq())
    // :+在Seq尾部添加元素
    triggers(klass) = existing :+ Trigger(threshold, action)
  }

  /**
   * Adds a trigger to be executed before the store is flushed. This normally happens before
   * closing, and is useful for flushing intermediate state to the store, e.g. when replaying
   * in-progress applications through the SHS.
    * => Unit 传名引用，表示传递一个方法块{}，该方法块不返回任何值
    * 因为加上了=>，传名，所以直接传递整个方法体进入函数里，如果是普通的传值，那么会先evaluate，然后将方法块值传入
   *
   * Flush triggers are called synchronously in the same thread that is closing the store.
   */
  def onFlush(action: => Unit): Unit = {
    // 将函数添加到trigger中去
    flushTriggers += { () => action }
  }

  /**
   * Enqueues an action to be executed asynchronously. The task will run on the calling thread if
   * `ASYNC_TRACKING_ENABLED` is `false`.
   */
  def doAsync(fn: => Unit): Unit = {
    // 调用java的executor的submit函数，并且返回值
    executor.submit(new Runnable() {
      // tryLog {fn} 其实就是tryLog(fn)，运行出了一个结果
      override def run(): Unit = Utils.tryLog { fn }
    })
  }

  override def read[T](klass: Class[T], naturalKey: Any): T = store.read(klass, naturalKey)

  override def write(value: Any): Unit = store.write(value)

  /** Write an element to the store, optionally checking for whether to fire triggers. */
  def write(value: Any, checkTriggers: Boolean): Unit = {
    write(value)

    if (checkTriggers && !stopped) {
      triggers.get(value.getClass()).foreach { list =>
        doAsync {
          val count = store.count(value.getClass())
          list.foreach { t =>
            if (count > t.threshold) {
              t.action(count)
            }
          }
        }
      }
    }
  }

  override def delete(klass: Class[_], naturalKey: Any): Unit = store.delete(klass, naturalKey)

  override def getMetadata[T](klass: Class[T]): T = store.getMetadata(klass)

  override def setMetadata(value: Any): Unit = store.setMetadata(value)

  override def view[T](klass: Class[T]): KVStoreView[T] = store.view(klass)

  override def count(klass: Class[_]): Long = store.count(klass)

  override def count(klass: Class[_], index: String, indexedValue: Any): Long = {
    store.count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    close(true)
  }

  /** A close() method that optionally leaves the parent store open. */
  // 关闭是同步方法，因为都要访问共同的资源
  def close(closeParent: Boolean): Unit = synchronized {
    if (stopped) {
      return
    }

    stopped = true
    executor.shutdown()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }

    // 结束的时候执行所有的trigger！！
    flushTriggers.foreach { trigger =>
      Utils.tryLog(trigger())
    }

    if (closeParent) {
      store.close()
    }
  }

  // 原来trigger的定义很简单，一个用于控制trigger的状态变量和一个触发操作
  private case class Trigger[T](
      threshold: Long,
      action: Long => Unit)

}
