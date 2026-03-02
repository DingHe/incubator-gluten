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
package org.apache.gluten.backendsapi

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext
// ListenerApi 是一个生命周期扩展接口。
// 它允许不同的后端（如 Velox, ClickHouse）在 Spark 驱动端（Driver）和执行端（Executor）的生命周期关键节点注入自定义的初始化或清理逻辑。
// 后端特定初始化：不同的 Native 引擎需要不同的启动环境。例如，Velox 可能需要初始化特定的内存池，而 ClickHouse 可能需要加载特定的配置文件。
// 解耦生命周期管理：SubstraitBackend 负责调用这些钩子，但它并不知道具体的逻辑。具体的后端实现类通过实现 ListenerApi 来定义自己该干什么。
// 资源回收：确保在 Spark 应用程序结束或 Executor 退出时，Native 层的非托管资源（如网络连接、临时文件或句柄）能够被优雅地释放。
trait ListenerApi {
  // 触发时机：在 Spark Driver 进程启动，且 Gluten 插件被加载时。
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  // 触发时机：当 Spark Context 停止（Driver 关闭）时。
  def onDriverShutdown(): Unit = {}
  // 触发时机：在每个 Spark Executor 进程启动时调用。
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}
}
