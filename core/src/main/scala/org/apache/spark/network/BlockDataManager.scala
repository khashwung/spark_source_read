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

package org.apache.spark.network

import scala.reflect.ClassTag

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{BlockId, StorageLevel}

private[spark]
trait BlockDataManager {

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
    * 获取local block数据，所以每个block由BlockId唯一标识！！！
    * 获得是ManagedBuffer,这是一个immutable view！！（为啥immutable？因为只允许读啊，不允许修改），然后
    * 这个类提供了获取InputStream的方法！！
   */
  def getBlockData(blockId: BlockId): ManagedBuffer

  /**
   * Put the block locally, using the given storage level.
   *
   * Returns true if the block was stored and false if the put operation failed or the block
   * already existed.
    * 根据storage level存储block数据，如果成功存储返回true，如果存储失败或者已经有了这个blockId，就返回false
    * StorageLevel也是一个对象哦
   */
  def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean

  /**
   * Release locks acquired by [[putBlockData()]] and [[getBlockData()]].
    * ????没太明白
   */
  def releaseLock(blockId: BlockId, taskAttemptId: Option[Long]): Unit
}
