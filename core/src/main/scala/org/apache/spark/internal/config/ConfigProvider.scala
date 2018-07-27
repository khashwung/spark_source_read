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

package org.apache.spark.internal.config

import java.util.{Map => JMap}

import org.apache.spark.SparkConf

/**
 * A source of configuration values.
  * 定义了一个配置数据源的服务接口,提供给外部的代码使用任何获得配置信息，而不用管从哪个源来的
 */
private[spark] trait ConfigProvider {

  def get(key: String): Option[String]

}

// 从sys.env中读，注意：env是指操作系统的环境变量
// sys是scala提供的一个对象，里面包含若干系统的信息
private[spark] class EnvProvider extends ConfigProvider {

  override def get(key: String): Option[String] = sys.env.get(key)

}

// 从sys.props中读，注意：props指jvm虚拟机里面的环境变量，这里有三种设置方法：
// 1.jvm虚拟机内置；2.java -D添加的；3.System.setProperty设置
private[spark] class SystemProvider extends ConfigProvider {

  override def get(key: String): Option[String] = sys.props.get(key)

}

private[spark] class MapProvider(conf: JMap[String, String]) extends ConfigProvider {

  override def get(key: String): Option[String] = Option(conf.get(key))

}

/**
 * A config provider that only reads Spark config keys.
 */
private[spark] class SparkConfigProvider(conf: JMap[String, String]) extends ConfigProvider {

  override def get(key: String): Option[String] = {
    if (key.startsWith("spark.")) {
      Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf))
    } else {
      None
    }
  }

}
