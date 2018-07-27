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

package org.apache.spark.internal

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.impl.StaticLoggerBinder

import org.apache.spark.util.Utils

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
  * 所有想记录log的类，都可以使用该trait。
 */
trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  // @transient标识该字段在序列化时不用被序列化！！当然，JPA中也有@transient，用于model中表示该字段不用在数据库中建字段
  // 本质上还是使用了slf4j的Logger类
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  // 通过定义一个log对象获得私有变量log_
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      // 本质上还是使用了slf4j的LoggerFactory
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }
  // 以上全是对Logger的封装

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    initializeLogIfNecessary(isInterpreter, silent = false)
  }

  // necessary的意思是是不是已经初始化过了
  protected def initializeLogIfNecessary(
      isInterpreter: Boolean,
      silent: Boolean = false): Boolean = {
    // 双重锁！！确保多线程也是只会初始化一次，double check。同步代码块外一次check，同步代码块内部一次check
    if (!Logging.initialized) {
      // 同步代码块
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter, silent)
          return true
        }
      }
    }
    false
  }

  private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    if (Logging.isLog4j12()) {
      // log4f里的LogManager
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.defaultSparkLog4jConfig = true
        // 默认log4f的配置
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            if (!silent) {
              System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            }
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      val rootLogger = LogManager.getRootLogger()
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel()
      }

      // 是REPL环境么？如果是的，就采用if里面这个
      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val replLogger = LogManager.getLogger(logName)
        // 默认WARN，如果设置就是自己设置的logger等级，如果没有采用默认的WARN
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          if (!silent) {
            System.err.printf("Setting default log level to \"%s\".\n", replLevel)
            System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
              "For SparkR, use setLogLevel(newLevel).")
          }
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private[spark] object Logging {
  // 看来静态参数都放在对象这里面,和java的@volatile的意思一样，用于控制并发的
  @volatile private var initialized = false
  @volatile private var defaultRootLevel: Level = null
  @volatile private var defaultSparkLog4jConfig = false

  // 初始化个空对象，用作锁
  val initLock = new Object()

  // 初始化该Object的时候就加载这段，
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    // 使用反射获得类，这个classForName其实就是Class.forName的封装！！
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      // 安装的意思就是在Logger对象中中添加Handler！因为该方法没有参数，所以传递null
      bridgeClass.getMethod("install").invoke(null)
    }
    // 上面三个方法分别是，移除所有的Handler，然后确认没有安装的了，安装一下SLF4JBridgehandler
  } catch {
    // 没有该类
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }

  /**
   * Marks the logging system as not initialized. This does a best effort at resetting the
   * logging system to its initial state so that the next class to use logging triggers
   * initialization again.
   */
  def uninitialize(): Unit = initLock.synchronized {
    if (isLog4j12()) {
      if (defaultSparkLog4jConfig) {
        defaultSparkLog4jConfig = false
        LogManager.resetConfiguration()
      } else {
        LogManager.getRootLogger().setLevel(defaultRootLevel)
      }
    }
    this.initialized = false
  }

  private def isLog4j12(): Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // 是不是这个类？ 大量应用反射
    "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
  }
}
