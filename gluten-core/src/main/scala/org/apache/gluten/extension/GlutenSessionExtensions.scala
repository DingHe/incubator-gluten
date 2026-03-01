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
package org.apache.gluten.extension

import org.apache.gluten.component.Component
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
// GlutenSessionExtensions 是整个 Apache Gluten 项目在 Spark 端的启动开关和集成中心。它通过扩展 Spark 的 SparkSessionExtensions 机制，将所有的 Native 优化逻辑“注入”到 Spark 的查询引擎中。
// 该类的主要职责是作为 Plugin 的引导程序：
// 接入 Spark 扩展机制：它继承自 (SparkSessionExtensions => Unit)，这是 Spark 官方提供的插件切入点，允许第三方库修改 SQL 解析、优化和物理执行计划。
// 管理全局控制逻辑：它定义了在什么情况下 Gluten 应该生效，或者在什么情况下回退到原生 Spark。
// 组件化注入（Component-based Injection）：它不直接硬编码某个后端的规则（如 Velox 或 ClickHouse），而是遍历所有已注册的组件，让它们按优先级将各自的规则注入到系统中。
// 统一开关管理：通过 InjectorControl，它统一了基于配置和基于线程本地变量（ThreadLocal）的动态开关逻辑。
private[gluten] class GlutenSessionExtensions
  extends (SparkSessionExtensions => Unit)
  with Logging {
  import GlutenSessionExtensions._
  // 这是该类的核心入口方法。当 Spark 启动并加载配置的扩展时，会调用此方法。
  override def apply(exts: SparkSessionExtensions): Unit = {
    // 创建一个注入器实例，负责后续规则、策略、函数向 Spark 的挂载。
    val injector = new Injector(exts)
    injector.control.disableOn {
      session =>
        // 检查 Spark 配置项 spark.gluten.enabled
        // 逻辑：如果该配置为 false，则通过 InjectorControl 禁用所有已注入的 Gluten 规则，使查询完全走原生 Spark 路径。
        val glutenEnabledGlobally = session.conf
          .get(
            GlutenCoreConfig.GLUTEN_ENABLED.key,
            GlutenCoreConfig.GLUTEN_ENABLED.defaultValueString)
          .toBoolean
        val disabled = !glutenEnabledGlobally
        logDebug(s"Gluten is disabled by variable: glutenEnabledGlobally: $glutenEnabledGlobally")
        disabled
    }
    // 支持在单个线程（如某个特定的 Query 或 Job）中临时开关 Gluten。
    injector.control.disableOn {
      session =>
        val glutenEnabledForThread =
          Option(session.sparkContext.getLocalProperty(GLUTEN_ENABLE_FOR_THREAD_KEY))
            .forall(_.toBoolean)
        val disabled = !glutenEnabledForThread
        logDebug(s"Gluten is disabled by variable: glutenEnabledForThread: $glutenEnabledForThread")
        disabled
    }
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    // 倒序注入组件规则：
    Component.sorted().reverse.foreach(_.injectRules(injector))
    injector.inject()
  }
}

object GlutenSessionExtensions {
  val GLUTEN_SESSION_EXTENSION_NAME: String = classOf[GlutenSessionExtensions].getCanonicalName
  val GLUTEN_ENABLE_FOR_THREAD_KEY: String = "gluten.enabledForCurrentThread"
}
