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

import org.apache.gluten.GlutenBuildInfo
import org.apache.gluten.backend.Backend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED
import org.apache.gluten.events.GlutenBuildInfoEvent
import org.apache.gluten.extension.columnar.LoggedRule
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.softaffinity.SoftAffinityListener
import org.apache.spark.sql.execution.adaptive.GlutenCostEvaluator
import org.apache.spark.sql.execution.ui.{GlutenSQLAppStatusListener, GlutenUIUtils}
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

// 定义了基于 Substrait 协议（一种跨语言的关系代数中间表示）的后端在物理执行过程中的通用行为。
// 简单来说，如果你想为 Gluten 实现一个新的计算引擎（如 Velox、ClickHouse 或 DuckDB），通常都需要继承这个类。
// Substrait 协议标准化：它假设后端通过 Substrait 计划进行通信。它定义了如何将 Spark 算子校验并转化为 Substrait 消息的 API 框架。
// 生命周期管理：负责处理 Spark Driver 端和 Executor 端启动/关闭时的逻辑挂载。
// API 门面（Facade）模式：它将复杂的后端逻辑拆分为多个专用的 Api 模块（如 ValidatorApi, IteratorApi 等），强制子类实现这些模块以保证功能完整性。
// 监控与集成：负责将 Gluten 特有的 UI 标签、指标和构建信息（Version/Commit）集成到 Spark UI 和日志中。
trait SubstraitBackend extends Backend with Logging {
  import SubstraitBackend._
  // 在内部缓存 SparkContext 引用。
  private var _sc: Option[SparkContext] = None

  final override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    _sc = Some(sc)
    val conf = pc.conf()

    // Register Gluten listeners
    // 注册 GlutenSQLAppStatusListener（用于收集 SQL 统计）。
    GlutenSQLAppStatusListener.register(sc)
    // 根据配置决定是否注册 SoftAffinityListener（软亲和力调度）。
    if (conf.get(GLUTEN_SOFT_AFFINITY_ENABLED)) {
      SoftAffinityListener.register(sc)
    }

    postBuildInfoEvent(sc)

    setPredefinedConfigs(conf)

    listenerApi().onDriverStart(sc, pc)
  }

  final override def onDriverShutdown(): Unit = {
    listenerApi().onDriverShutdown()
  }
  final override def onExecutorStart(pc: PluginContext): Unit = {
    listenerApi().onExecutorStart(pc)
  }
  final override def onExecutorShutdown(): Unit = {
    listenerApi().onExecutorShutdown()
  }
  final override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectRuleWrapper(r => new LoggedRule(r))
    injector.gluten.ras.injectRuleWrapper(r => new LoggedRule(r))
    ruleApi().injectRules(injector)
  }

  final override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    _sc.foreach {
      sc =>
        if (GlutenUIUtils.uiEnabled(sc)) {
          GlutenUIUtils.attachUI(sc)
          logInfo("Gluten SQL Tab has been attached.")
        }
    }
  }

  def iteratorApi(): IteratorApi
  def sparkPlanExecApi(): SparkPlanExecApi
  def transformerApi(): TransformerApi
  def validatorApi(): ValidatorApi
  def metricsApi(): MetricsApi
  def listenerApi(): ListenerApi
  def ruleApi(): RuleApi
  def settings(): BackendSettingsApi
}

object SubstraitBackend extends Logging {

  /** Since https://github.com/apache/incubator-gluten/pull/2247. */
  // 主要负责 Gluten 运行时版本信息的采集、记录与展示。
  // 它的核心作用是在 Spark 任务启动时，清晰地定义当前运行的 Gluten 是“谁”（版本、源码分支）以及它是“如何生成的”（编译环境）。这对于分布式系统中的版本对齐和故障排查至关重要。
  private def postBuildInfoEvent(sc: SparkContext): Unit = {
    // export gluten version to property to spark
    // 作用：将 Gluten 的版本号写入 Java 系统属性。
    System.setProperty("gluten.version", GlutenBuildInfo.VERSION)

    val glutenBuildInfo = new mutable.LinkedHashMap[String, String]()

    glutenBuildInfo.put("Gluten Version", GlutenBuildInfo.VERSION)
    glutenBuildInfo.put("GCC Version", GlutenBuildInfo.GCC_VERSION)
    glutenBuildInfo.put("Java Version", GlutenBuildInfo.JAVA_COMPILE_VERSION)
    glutenBuildInfo.put("Scala Version", GlutenBuildInfo.SCALA_COMPILE_VERSION)
    glutenBuildInfo.put("Spark Version", GlutenBuildInfo.SPARK_COMPILE_VERSION)
    glutenBuildInfo.put("Hadoop Version", GlutenBuildInfo.HADOOP_COMPILE_VERSION)
    glutenBuildInfo.put("Gluten Branch", GlutenBuildInfo.BRANCH)
    glutenBuildInfo.put("Gluten Revision", GlutenBuildInfo.REVISION)
    glutenBuildInfo.put("Gluten Revision Time", GlutenBuildInfo.REVISION_TIME)
    glutenBuildInfo.put("Gluten Build Time", GlutenBuildInfo.BUILD_DATE)
    glutenBuildInfo.put("Gluten Repo URL", GlutenBuildInfo.REPO_URL)

    val loggingInfo = glutenBuildInfo
      .map { case (name, value) => s"$name: $value" }
      .mkString(
        "Gluten build info:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
    if (GlutenUIUtils.uiEnabled(sc)) {
      val event = GlutenBuildInfoEvent(glutenBuildInfo.toMap)
      GlutenUIUtils.postEvent(sc, event)
    }
  }
  // 其核心作用是：在运行时强制改写 Spark 的配置项（SparkConf），以确保 Spark 的原生行为不会干扰 Gluten 的执行，并启用 Gluten 特有的优化组件。
  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // adaptive custom cost evaluator class
    // 将 Spark AQE（自适应查询执行）中的代价评估模型替换为 Gluten 自定义的 GlutenCostEvaluator
    val enableGlutenCostEvaluator = conf.get(GlutenConfig.COST_EVALUATOR_ENABLED)
    if (enableGlutenCostEvaluator) {
      conf.set(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS, classOf[GlutenCostEvaluator].getName)
    }

    // Disable vanilla columnar readers, to prevent columnar-to-columnar conversions.
    // FIXME: Do we still need this trick since
    //  https://github.com/apache/incubator-gluten/pull/1931 was merged?
    // 禁用 Spark 原生向量化读取器 (Vanilla Vectorized Readers)
    // 强制关闭 Spark 原生的 Parquet、ORC 和 Cache 的向量化读取功能。
    // 防止格式冲突：Spark 原生的向量化读取器产生的 ColumnarBatch 是基于 Spark JVM 内存布局的。如果开启了它，后续传给 Gluten Native 引擎时，可能需要进行一次昂贵的“列转列（Columnar-to-Columnar）”重新布局。
    if (!conf.get(GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED)) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue is fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED, false)
      conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED, false)
      conf.set(SQLConf.CACHE_VECTORIZED_READER_ENABLED, false)
    }
  }
}
