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
package org.apache.gluten

import org.apache.gluten.component.Component
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.initializer.CodedInputStreamClassInitializer
import org.apache.gluten.task.TaskListener

import org.apache.spark.{SparkConf, SparkContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SparkResourceUtil

import java.util
import java.util.Collections

// Gluten 是一个旨在通过利用本地库（如 Velox 或 ClickHouse）来加速 Apache Spark 执行的中间件项目。
// GlutenPlugin 实现了 Spark 的 SparkPlugin 接口。它的主要作用是将 Gluten 的功能集成到 Spark 运行周期中。
// 桥梁作用：它是连接 Spark 原生环境与 Gluten 本地执行引擎（Native Engine）的纽带。
// 分发角色：它负责在 Spark 集群的 Driver 端（控制面）和 Executor 端（数据面）分别启动对应的组件，确保查询计划的重写（Driver）和列式执行引擎的初始化（Executor）能够同步进行。
class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    // 返回 GlutenDriverPlugin 实例。Spark Driver 启动时会调用此方法
    new GlutenDriverPlugin()
  }
  // 返回 GlutenExecutorPlugin 实例。Spark 每个 Executor 启动时会调用此方法。
  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}
// 负责在 Driver 端进行全局配置注入和生命周期管理。
private[gluten] class GlutenDriverPlugin extends DriverPlugin with Logging {
  import GlutenDriverPlugin._
  // init 方法是在 Driver 端 初始化 Gluten 核心逻辑的入口
  // 核心任务是：确保 Gluten 的扩展规则被注入到 Spark 中，并启动 Gluten 的各个子组件。
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    // 获取当前 Spark 应用的配置对象（SparkConf）
    val conf = pluginContext.conf()
    // Spark SQL extensions
    // 读取当前配置中 spark.sql.extensions 的值
    // 背景：这个配置项可能已经包含了其他插件（如 Iceberg 或 Delta Lake）的扩展类名。SPARK_SESSION_EXTENSIONS 是spark StaticSQLConf 中定义的 Key。
    val extensionSeq = conf.get(SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty)
    // 作用：这保证了即使你忘记在命令行设置 --conf spark.sql.extensions=...
    // 只要你启用了 Gluten 的 Plugin，它也会自动强制注入自己的 SQL 扩展规则（如 ColumnarRule 等）。
    if (!extensionSeq.toSet.contains(GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)) {
      conf.set(
        SPARK_SESSION_EXTENSIONS,
        extensionSeq :+ GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)
    }
    // 调用内部方法设置一些 Gluten 运行所需的预设参数。
    // 作用：Gluten 需要一些特定的 Spark 配置才能发挥性能（例如强制开启 AQE 或特定的内存分配方案）。此方法会覆盖或设置这些“最佳实践”参数，减少用户手动配置的成本。
    setPredefinedConfigs(conf)
    // 解读：加载 Gluten 的各个功能组件（如内存管理组件、后端引擎适配组件等），并根据定义的优先级进行排序。
    val components = Component.sorted()
    // 解读：在 Driver 日志中打印当前已加载并排序好的组件列表。
    printComponentInfo(components)
    // 遍历所有组件，并依次调用它们的 onDriverStart 方法
    components.foreach(_.onDriverStart(sc, pluginContext))
    // 返回空 Map
    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    Component.sorted().foreach(_.registerMetrics(appId, pluginContext))
  }

  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onDriverShutdown())
  }
}
// Gluten 依赖原生后端（如 Velox）在**堆外内存（Off-heap Memory）**执行计算，因此这段代码主要围绕如何科学地分配和校验堆外内存展开。
private object GlutenDriverPlugin extends Logging {
  // 作用是确保 Spark 的堆外内存配置满足 Gluten 运行的最低要求
  private def checkOffHeapSettings(conf: SparkConf): Unit = {
    // 如果开启了 DYNAMIC_OFFHEAP_SIZING_ENABLED（动态堆外内存调整），则不检查，因为后续会自动计算。
    if (conf.get(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED)) {
      // When dynamic off-heap sizing is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }
    // 如果开启了 COLUMNAR_MEMORY_UNTRACKED（不追踪内存模式），也不检查，通常用于极端的自定义场景。
    if (conf.get(GlutenCoreConfig.COLUMNAR_MEMORY_UNTRACKED)) {
      // When untracked memory mode is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }
    // 检查 spark.memory.offHeap.enabled 是否为 true
    // 检查 spark.memory.offHeap.size 是否大于等于 1MB
    val minOffHeapSize = "1MB"
    if (
      !conf.getBoolean(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, defaultValue = false) ||
      conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, 0) < JavaUtils.byteStringAsBytes(
        minOffHeapSize)
    ) {
      throw new GlutenException(
        s"Must set '${GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY}' to true " +
          s"and set '${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' to be greater " +
          s"than $minOffHeapSize")
    }
  }
  // 核心内存计算逻辑
  // Gluten 性能优化的精髓，它负责计算原生后端实际可用的字节数，并将结果注入到 Gluten 内部参数中。
  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // check memory off-heap enabled and size.
    checkOffHeapSettings(conf)

    // Get the off-heap size set by user.
    val offHeapSize =
      if (conf.getBoolean(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, false)) {
        // 如果用户开启了动态调整功能，Gluten 会接管堆外内存的控制权：
        val onHeapSize: Long =
          if (conf.contains(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)) {
            conf.getSizeAsBytes(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)
          } else {
            // 1GB default
            1024 * 1024 * 1024
          }

        if (conf.contains(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY)) {
          logWarning(
            s"Dynamic off-heap sizing is enabled. Ignoring user-defined " +
              s"'${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' setting.")
        }
        if (conf.contains(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY)) {
          logWarning(
            s"Dynamic off-heap sizing is enabled. Ignoring user-defined " +
              s"'${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' setting.")
        }
        // 强制关闭 Spark 原生堆外：将 Spark 自身的 offHeap.enabled 设为 false，
        // 这是为了防止 Spark 的 MemoryManager 和 Gluten 的原生内存管理器产生资源争抢。
        conf.set(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, "0")
        conf.set(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "false")

        ((onHeapSize - (300 * 1024 * 1024)) *
          conf.getDouble(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION.key, 0.6d)).toLong
      } else {
        conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY)
      }

    // Set off-heap size in bytes.
    conf.set(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES, offHeapSize)

    // Set off-heap size in bytes per task.
    // 通过 SparkResourceUtil 获取每个 Executor 的并行度（通常是 spark.executor.cores / spark.task.cpus）。
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)
    conf.set(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR, taskSlots)
    val offHeapPerTask = offHeapSize / taskSlots
    conf.set(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES, offHeapPerTask)

    // Pessimistic off-heap sizes, with the assumption that all non-borrowable storage memory
    // determined by spark.memory.storageFraction was used.
    val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
    val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
    conf.set(
      GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES,
      conservativeOffHeapPerTask)
  }

  private def printComponentInfo(components: Seq[Component]): Unit = {
    val loggingInfo = components
      .map {
        c =>
          val infoStr =
            if (c.info().isEmpty) ""
            else "\n" + c.info().map { case (k, v) => s"  $k = $v" }.mkString("\n")
          s"Component ${c.name()}$infoStr"
      }
      .mkString(
        "Gluten components:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
  }
}
// 在分布式计算中，Executor 负责实际的数据处理。该插件的主要职责是初始化本地 C++ 引擎（Native Engine）并管理每个任务（Task）的资源生命周期。
private[gluten] class GlutenExecutorPlugin extends ExecutorPlugin {
  // 定义了一组任务监听器，用于在任务执行的不同阶段执行特定的逻辑
  // 初始化为 Seq(TaskResources)。TaskResources 是 Gluten 中非常关键的一个组件，它负责管理 Task 级别的内存分配器（Memory Allocator）和本地资源句柄。
  // 通过这个监听器，Gluten 能够确保 C++ 层分配的内存在任务结束（无论成功或失败）时被正确回收，防止内存泄漏。
  private val taskListeners: Seq[TaskListener] = Seq(TaskResources)

  /** Initialize the executor plugin. */
  // 在 Executor 进程启动时进行初始化操作
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // 修改 Protobuf 限制
    // 由于 Gluten 在 Java 和 C++ 之间传递复杂的执行计划（Plan）时使用 Protobuf 序列化，默认的递归深度限制可能不足以处理极其复杂的 SQL 查询。
    // 此操作通过反射等方式绕过限制，防止解析深层嵌套计划时崩溃。
    CodedInputStreamClassInitializer.modifyDefaultRecursionLimitUnsafe
    // Initialize Backend.
    // 按顺序启动所有注册的组件（如 Velox 引擎后端或 ClickHouse 引擎后端），加载动态库（.so/.dylib），并初始化全局的本地内存池。
    Component.sorted().foreach(_.onExecutorStart(ctx))
  }

  /** Clean up and terminate this plugin. For example: close the native engine. */
  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onExecutorShutdown())
    super.shutdown()
  }
  // 在每个具体的 Task 开始执行前触发
  override def onTaskStart(): Unit = {
    taskListeners.foreach(_.onTaskStart())
  }

  override def onTaskSucceeded(): Unit = {
    taskListeners.reverse.foreach(_.onTaskSucceeded())
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    taskListeners.reverse.foreach(_.onTaskFailed(failureReason))
  }
}

private object GlutenPlugin {}
