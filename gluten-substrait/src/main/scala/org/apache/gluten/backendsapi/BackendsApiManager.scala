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

import org.apache.gluten.component.Component
// 在 Apache Gluten 项目中，BackendsApiManager 是一个核心的单例管理对象。它是整个 Gluten 架构中 “插件化（Pluggable）” 设计的精髓所在。
// Gluten 的目标是提供一个通用的中间层（基于 Substrait），让 Spark 能够运行在不同的 Native 引擎上（如 Velox、ClickHouse）。
// BackendsApiManager 的核心作用是 “后端抽象层（Backend Abstraction Layer）”：
// 统一接口：它为上层逻辑（Java/Scala 端）提供了一套标准的 API 访问入口。
// 屏蔽差异：上层代码在转换算子、验证表达式或获取配置时，不需要知道当前运行的是哪个后端。它通过 BackendsApiManager 动态调用具体后端的实现。
// 依赖注入与发现：利用组件加载机制（Component Service Loader），在运行时自动探测并加载唯一的后端实现。
object BackendsApiManager {
  // 持有当前生效的后端实例。
  private lazy val backend: SubstraitBackend = initializeInternal()

  /** Initialize all backends apis. */
  // 执行真正的后端扫描和加载逻辑。
  private def initializeInternal(): SubstraitBackend = {
    // 获取所有已加载的组件。
    val loadedSubstraitBackends = Component.sorted().filter(_.isInstanceOf[SubstraitBackend])
    assert(
      loadedSubstraitBackends.size == 1,
      s"Zero or more than one Substrait backends are loaded: " +
        s"${loadedSubstraitBackends.map(_.name()).mkString(", ")}")
    // 返回找到的唯一后端实例。
    loadedSubstraitBackends.head.asInstanceOf[SubstraitBackend]
  }

  /** Automatically detect the backend api. */
 // 通常在 Spark Session 启动时调用，确保后端就绪并返回后端名称。
  def initialize(): String = {
    getBackendName
  }

  // Note: Do not make direct if-else checks based on output of the method.
  // Any form of backend-specific code should be avoided from appearing in common module
  // (e.g. gluten-substrait)
  def getBackendName: String = {
    backend.name()
  }

  def getListenerApiInstance: ListenerApi = {
    backend.listenerApi()
  }

  def getIteratorApiInstance: IteratorApi = {
    backend.iteratorApi()
  }

  def getSparkPlanExecApiInstance: SparkPlanExecApi = {
    backend.sparkPlanExecApi()
  }

  def getTransformerApiInstance: TransformerApi = {
    backend.transformerApi()
  }

  def getValidatorApiInstance: ValidatorApi = {
    backend.validatorApi()
  }

  def getMetricsApiInstance: MetricsApi = {
    backend.metricsApi()
  }

  def getRuleApiInstance: RuleApi = {
    backend.ruleApi()
  }

  def getSettings: BackendSettingsApi = {
    backend.settings
  }
}
