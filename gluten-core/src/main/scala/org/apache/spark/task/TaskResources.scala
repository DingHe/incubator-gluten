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
package org.apache.spark.task

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.memory.SimpleMemoryUsageRecorder
import org.apache.gluten.task.TaskListener

import org.apache.spark.{TaskContext, TaskFailedReason, TaskKilledException, UnknownReason}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SparkTaskUtil, TaskCompletionListener, TaskFailureListener}

import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.compat.Platform.ConcurrentModificationException
// TaskResources 是 Apache Gluten 中一个至关重要的基础设施类，主要负责管理 Spark 任务（Task）执行期间的所有资源（尤其是内存和 Native 句柄）的生命周期。
// 由于 Gluten 的核心算子在 Native 层（C++）运行，Spark 原生的 JVM 内存管理无法自动释放这些非托管资源。
// TaskResources 建立了一套与 Spark TaskContext 绑定的注册机制，确保无论任务成功还是失败，所有 Native 资源都能被准确释放。
object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  // 从配置中读取，决定是否开启内存调试模式。如果开启，通常会记录内存分配的堆栈信息。
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  private def newUnsafeTaskContext(properties: Properties): TaskContext = {
    SparkTaskUtil.createTestTaskContext(properties)
  }

  implicit private class PropertiesOps(properties: Properties) {
    def setIfMissing(key: String, value: String): Unit = {
      if (!properties.containsKey(key)) {
        properties.setProperty(key, value)
      }
    }
  }

  private def setUnsafeTaskContext(): Unit = {
    if (inSparkTask()) {
      throw new UnsupportedOperationException(
        "TaskResources#setUnsafeTaskContext should only be called outside Spark task")
    }
    val properties = new Properties()
    SQLConf.get.getAllConfs.foreach {
      case (key, value) if key.startsWith("spark") =>
        properties.put(key, value)
      case _ =>
    }
    properties.setIfMissing(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "true")
    properties.setIfMissing(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, "1TB")
    TaskContext.setTaskContext(newUnsafeTaskContext(properties))
  }

  private def unsetUnsafeTaskContext(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException()
    }
    if (getLocalTaskContext().taskAttemptId() != -1) {
      throw new IllegalStateException()
    }
    TaskContext.unset()
  }

  // Run code with unsafe task context. If the call took place from Spark driver or test code
  // without a Spark task context registered, a temporary unsafe task context instance will
  // be created and used. Since unsafe task context is not managed by Spark's task memory manager,
  // Spark may not be aware of the allocations happened inside the user code.
  //
  // The API should typically be used in the following cases:
  //
  // 1. Run code on driver
  // 2. Run test code
  def runUnsafe[T](body: => T): T = {
    if (inSparkTask()) {
      return body
    }
    TaskResources.setUnsafeTaskContext()
    onTaskStart()
    val context = getLocalTaskContext()
    try {
      val out =
        try {
          body
        } catch {
          case t: Throwable =>
            // Similar code with those in Task.scala
            try {
              context.markTaskFailed(t)
            } catch {
              case t: Throwable =>
                t.addSuppressed(t)
            }
            context.markTaskCompleted(Some(t))
            throw t
        } finally {
          try {
            context.markTaskCompleted(None)
          } finally {
            TaskResources.unsetUnsafeTaskContext()
          }
        }
      onTaskSucceeded()
      out
    } catch {
      case t: Throwable =>
        onTaskFailed(UnknownReason)
        throw t
    }
  }

  private val RESOURCE_REGISTRIES =
    new java.util.IdentityHashMap[TaskContext, TaskResourceRegistry]()
  // 获取spark执行的上下文
  def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }
  // 用于判断当前代码的执行上下文是否处于一个由 Spark 管理的 Executor Task（执行任务） 线程中。
  // 这是 Spark 原生的静态方法。在 Spark 的架构中，当 Executor 启动一个 Task 线程来处理数据分区时，它会通过 ThreadLocal 变量在该线程中设置一个 TaskContext 对象。
  def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def getTaskResourceRegistry(): TaskResourceRegistry = {
    if (!inSparkTask()) {
      throw new UnsupportedOperationException(
        "Not in a Spark task. If the code is running on driver or for testing purpose, " +
          "try using TaskResources#runUnsafe")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {
      if (!RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "" +
            "TaskResourceRegistry is not initialized, please ensure TaskResources " +
            "is added to GlutenExecutorPlugin's task listener list")
      }
      return RESOURCE_REGISTRIES.get(tc)
    }
  }

  def addRecycler(name: String, prio: Int)(f: => Unit): Unit = {
    addAnonymousResource(new TaskResource {
      override def release(): Unit = f

      override def priority(): Int = prio

      override def resourceName(): String = name
    })
  }

  def addResource[T <: TaskResource](id: String, resource: T): T = {
    getTaskResourceRegistry().addResource(id, resource)
  }

  def releaseResource(id: String): Unit = {
    getTaskResourceRegistry().releaseResource(id)
  }

  def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T = {
    getTaskResourceRegistry().addResourceIfNotRegistered(id, factory)
  }

  def addAnonymousResource[T <: TaskResource](resource: T): T = {
    getTaskResourceRegistry().addResource(UUID.randomUUID().toString, resource)
  }

  def isResourceRegistered(id: String): Boolean = {
    getTaskResourceRegistry().isResourceRegistered(id)
  }

  def getResource[T <: TaskResource](id: String): T = {
    getTaskResourceRegistry().getResource(id)
  }

  def getSharedUsage(): SimpleMemoryUsageRecorder = {
    getTaskResourceRegistry().getSharedUsage()
  }
  // Gluten 内存与资源管理框架的生命周期初始化锚点。
  // 它的核心任务是：当一个 Spark 任务启动时，为其创建一个独立的“资源管家”，并挂载清理钩子。
  // 该方法由 GlutenExecutorPlugin 在 Task 启动时调用。它首先检查当前线程是否持有 TaskContext。如果没有，说明不在合法的 Spark 任务中，禁止初始化 Gluten 的资源框架，防止资源失去追踪。
  override def onTaskStart(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    // 获取当前 Task 的上下文
    val tc = getLocalTaskContext()
    // 对全局 Map 加锁，确保多线程下注册表的安全操作
    RESOURCE_REGISTRIES.synchronized {
      if (RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "TaskResourceRegistry is already initialized, this should not happen")
      }
      // 为该 Task 创建一个全新的资源注册表
      val registry = new TaskResourceRegistry
      RESOURCE_REGISTRIES.put(tc, registry)
      // 向 Spark 注册一个失败回调。注释中提到“防止在 Completion Listener 崩溃时错误被吞掉”。
      // 这里主要用于诊断，确保 Native 层抛出的崩溃信息能在日志中被准确捕获。
      tc.addTaskFailureListener(
        // in case of crashing in task completion listener, errors may be swallowed
        new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            // TODO:
            // The general duty of printing error message should not reside in memory module
            error match {
              case e: TaskKilledException if e.reason == "another attempt succeeded" =>
              case _ => logError(s"Task ${context.taskAttemptId()} failed by error: ", error)
            }
          }
        })
      // 注入完成监听器 (Completion Listener) - 最关键部分
      tc.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          RESOURCE_REGISTRIES.synchronized {
            val currentTaskRegistries = RESOURCE_REGISTRIES.get(context)
            if (currentTaskRegistries == null) {
              throw new IllegalStateException(
                "TaskResourceRegistry is not initialized, this should not happen")
            }
            // We should first call `releaseAll` then remove the registries, because
            // the functions inside registries may register new resource to registries.
            // 【核心】调用 releaseAll() 释放该 Task 注册的所有资源
            currentTaskRegistries.releaseAll()
            // 更新 Spark 的 Task 指标：将 Gluten 记录的 Native 内存峰值反馈给 Spark
            context.taskMetrics().incPeakExecutionMemory(registry.getSharedUsage().peak())
            // 从全局 Map 中移除该 Task 的注册表，防止内存泄漏
            RESOURCE_REGISTRIES.remove(context)
          }
        }
      })
    }
  }

  private def onTaskExit(): Unit = {
    // no-op
  }

  override def onTaskSucceeded(): Unit = {
    onTaskExit()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    onTaskExit()
  }
}

// thread safe
// TaskResourceRegistry 是 Gluten 资源管理系统的“仓库管理员”。它的核心职责是维护一个任务（Task）内所有资源的映射关系，并确保在任务结束时，这些资源能够按照正确的依赖顺序被安全释放。
// 在 Native 开发中，释放顺序至关重要。例如：必须先关闭使用内存的“执行算子”，才能释放底层的“内存池”。
class TaskResourceRegistry extends Logging {
  // 一个共享的内存使用记录器。
  private val sharedUsage = new SimpleMemoryUsageRecorder()
  // 资源 ID 到资源对象的映射。
  private val resources = mutable.Map.empty[String, TaskResource]
  // 优先级到资源集合的映射。
  private val priorityToResourcesMapping: mutable.Map[Int, mutable.LinkedHashSet[TaskResource]] =
    mutable.Map.empty[Int, mutable.LinkedHashSet[TaskResource]]
  // 专门用于检测在释放资源期间是否有非法修改动作。
  private var exclusiveLockAcquired: Boolean = false
  // 标准同步锁。
  private def lock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      body
    }
  }
  private def exclusiveLock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      exclusiveLockAcquired = true
      try {
        body
      } finally {
        exclusiveLockAcquired = false
      }
    }
  }

  private def addResource0(id: String, resource: TaskResource): Unit = lock {
    resources.put(id, resource)
    priorityToResourcesMapping
      .getOrElseUpdate(resource.priority(), mutable.LinkedHashSet.empty[TaskResource])
      .add(resource)
  }

  private def release(resource: TaskResource): Unit = exclusiveLock {
    // We disallow modification on registry's members when calling the user-defined release code.
    resource.release()
  }

  /** Release all managed resources according to priority and reversed order */
  private[task] def releaseAll(): Unit = lock {
    priorityToResourcesMapping.toSeq.sortBy(-_._1).foreach {
      case (_, resources) =>
        resources.toSeq.reverse.foreach(release)
    }
    priorityToResourcesMapping.clear()
    resources.clear()
  }

  /** Release single resource by ID */
  private[task] def releaseResource(id: String): Unit = lock {
    val resource = resources.getOrElse(
      id,
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id)))
    val samePrio = priorityToResourcesMapping.getOrElse(
      resource.priority(),
      throw new IllegalStateException("TaskResource's priority not found in priority mapping"))

    if (!samePrio.contains(resource)) {
      throw new IllegalStateException("TaskResource not found in priority mapping")
    }
    release(resource)
    samePrio.remove(resource)
    resources.remove(id)
  }

  private[task] def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T =
    lock {
      resources
        .getOrElse(
          id, {
            val resource = factory.apply()
            addResource0(id, resource)
            resource
          })
        .asInstanceOf[T]
    }

  private[task] def addResource[T <: TaskResource](id: String, resource: T): T = lock {
    if (resources.contains(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is already registered", id))
    }
    addResource0(id, resource)
    resource
  }

  private[task] def isResourceRegistered(id: String): Boolean = lock {
    resources.contains(id)
  }

  private[task] def getResource[T <: TaskResource](id: String): T = lock {
    resources
      .getOrElse(
        id,
        throw new IllegalArgumentException(
          String.format("TaskResource with ID %s is not registered", id)))
      .asInstanceOf[T]
  }

  private[task] def getSharedUsage(): SimpleMemoryUsageRecorder = lock {
    sharedUsage
  }
}
