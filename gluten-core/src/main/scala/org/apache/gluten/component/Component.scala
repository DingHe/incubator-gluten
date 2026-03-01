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
package org.apache.gluten.component

import org.apache.gluten.extension.columnar.cost.LongCoster
import org.apache.gluten.extension.columnar.transition.ConventionFunc
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable

/**
 * The base API to inject user-defined logic to Gluten. To register a component, the implementation
 * class of this trait should be placed to Gluten's classpath with a component file. Gluten will
 * discover all the component implementations then register them at the booting time.
 *
 * See [[Discovery]] to find more information about how the component files are handled.
 */
// 非常核心的插件化框架接口。它定义了如何向 Gluten 注入用户自定义逻辑（如不同的计算后端 Velox、ClickHouse，或特定的优化策略）
// Component 的核心作用是解耦与扩展：
// 统一接口：为所有 Gluten 的子模块（Backend、优化器、指标收集器等）提供统一的生命周期管理接口。
// 依赖管理：通过内置的依赖图（Graph）和拓扑排序，确保组件按照正确的顺序（先父后子）进行初始化和销毁。
// 运行时兼容性检查：支持在运行时根据环境（如是否存在某些 JAR 包）决定是否启用该组件。
// 逻辑注入：允许组件向 Spark 注入 SQL 规则、开销模型（Coster）以及转换逻辑（Convention）。
@Experimental
trait Component {
  import Component._
  // 每一个组件实例唯一的整数标识符，通过 nextUid 原子递增生成，用于在依赖图中唯一标识节点。
  private val uid = nextUid.getAndIncrement()
  // 一个 AtomicBoolean，确保每个组件实例只会被注册到全局依赖图中一次。
  private val isRegistered = new AtomicBoolean(false)
  // 将当前组件及其声明的依赖项加入到全局的 graph（依赖图）中
  final def ensureRegistered(): Unit = {
    if (!isRegistered.compareAndSet(false, true)) {
      return
    }
    graph.add(this)
    dependencies().foreach(req => graph.declareDependency(this, req))
  }

  /**
   * Determines whether a component should be registered based on runtime conditions. For instance,
   * if a component depends on a Spark extension's JAR, this method should be overridden to check
   * whether its core class (e.g., the extension class) is available in the runtime environment.
   */
  // 运行时兼容性检查。
  def isRuntimeCompatible: Boolean = {
    true
  }

  /** Base information. */
  // 返回组件的名称（用于日志和监控）
  def name(): String
  // 返回组件的详细信息（Map 形式），如版本号、编译时间等。
  def info(): Map[String, String] = Map.empty
  // 返回该组件所依赖的其他组件的类列表。
  def dependencies(): Seq[Class[_ <: Component]]

  /** Spark listeners. */
  // 在 Spark Driver 启动/关闭时执行的操作。
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  def onDriverShutdown(): Unit = {}
  // 在 Spark Executor 启动/关闭时执行的操作。
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}

  /** Metrics register, only called on Driver. */
  // 仅在 Driver 端调用，用于向 Spark 注册自定义的度量指标。
  def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {}

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns convention type it outputs,
   * and input conventions it requires.
   */
  // 用于重写 Gluten 的转换函数。
  // Gluten 需要知道一个计划是行存还是列存，组件可以通过此方法自定义判断逻辑。
  def convFuncOverride(): ConventionFunc.Override = ConventionFunc.Override.Empty

  /**
   * A sequence of [[org.apache.gluten.extension.columnar.cost.LongCoster]] Gluten is using for cost
   * evaluation.
   */
  // 返回一组开销计算器。
  // Gluten 的 CBO（基于代价的优化）会利用这些 Coster 来评估本地执行的成本。
  def costers(): Seq[LongCoster] = Nil

  /** Query planner rules. */
  // 核心扩展点。
  // 组件通过 injector 向 Spark 注入自定义的物理/逻辑优化规则。
  def injectRules(injector: Injector): Unit
}

object Component extends Logging {
  private val nextUid = new AtomicInteger()
  private val graph: Graph = new Graph()

  // format: off
  /**
   * Apply topology sort on all registered components in graph to get an ordered list of
   * components. The root nodes will be on the head side of the list, while leaf nodes
   * will be on the tail side of the list.
   *
   * Say if component-A depends on component-B while component-C requires nothing, then the
   * output order will be one of the following:
   *
   *   1. [component-B, component-A, component-C]
   *   2. [component-C, component-B, component-A]
   *   3. [component-B, component-C, component-A]
   *
   * By all means component B will be placed before component A because component B is a declared
   * dependency of component A.
   *
   * @throws UnsupportedOperationException When cycles in dependency graph are found.
   */
  // format: on
  def sorted(): Seq[Component] = {
    ensureAllComponentsRegistered()
    graph.sorted()
  }

  private[component] def sortedUnsafe(): Seq[Component] = {
    graph.sorted()
  }
  // Registry 类在 Gluten 插件系统中充当**“组件元数据仓库”**。
  // 核心目标是：
  // 双索引存储：同时通过 UID（唯一 ID）和 Class（类名）两种方式来索引和访问同一个组件。
  // 确保唯一性：通过严格的检查，防止同一个组件类或同一个 ID 被重复注册，保证系统结构的确定性。
  // 线程安全：由于组件注册可能发生在多线程环境（如异步加载），它通过 synchronized 确保了所有操作的原子性。
  private class Registry {
    // 建立从整数 ID 到组件对象的映射。
    // 这主要用于依赖图（Graph）中的节点计算，因为图算法通常处理数字索引效率更高。
    private val lookupByUid: mutable.Map[Int, Component] = mutable.Map()
    // 建立从 Scala/Java 类类型到组件对象的映射。这使得开发者可以方便地声明“我依赖 VeloxBackend 类”，而无需知道该类的具体 ID。
    private val lookupByClass: mutable.Map[Class[_ <: Component], Component] = mutable.Map()
    // 将一个新组件实例存入注册表。
    def register(comp: Component): Unit = synchronized {
      val uid = comp.uid
      val clazz = comp.getClass
      require(!lookupByUid.contains(uid), s"Component UID $uid already registered: ${comp.name()}")
      require(
        !lookupByClass.contains(clazz),
        s"Component class $clazz already registered: ${comp.name()}")
      lookupByUid(uid) = comp
      lookupByClass(clazz) = comp
    }

    def isUidRegistered(uid: Int): Boolean = synchronized {
      lookupByUid.contains(uid)
    }

    def isClassRegistered(clazz: Class[_ <: Component]): Boolean = synchronized {
      lookupByClass.contains(clazz)
    }

    def findByClass(clazz: Class[_ <: Component]): Component = synchronized {
      require(lookupByClass.contains(clazz))
      lookupByClass(clazz)
    }

    def findByUid(uid: Int): Component = synchronized {
      require(lookupByUid.contains(uid))
      lookupByUid(uid)
    }

    def allUids(): Seq[Int] = synchronized {
      return lookupByUid.keys.toSeq
    }
  }
  // Gluten 组件系统的核心调度器，负责处理组件之间的依赖关系、检测循环依赖并决定最终的初始化顺序
  // Graph 类的本质是一个有向无环图 (DAG) 管理器。
  // 在 Gluten 启动时，会有多个后端（如 Velox, ClickHouse）或优化组件注册进来。
  // 这些组件通常有严格的先后顺序（例如：必须先初始化内存管理组件，才能初始化执行引擎组件）。Graph 类通过拓扑排序算法，将这些杂乱的依赖关系梳理成一个线性的、可执行的列表。
  private class Graph {
    import Graph._
    // 负责 UID 与组件对象、类与组件对象之间的双向映射查找。
    private val registry: Registry = new Registry()
    // 存储“原始依赖对”（组件 UID -> 依赖的类）。它是构建图的原始素材。
    private val uidAndDependencyPairs: mutable.Buffer[(Int, Class[_ <: Component])] =
      mutable.Buffer()

    // 一旦排序完成，结果会存储在这里。
    // 如果后续有新组件加入（调用了 add 或 declareDependency），该缓存会被置为 None 触发重新计算。
    private var sortedComponents: Option[Seq[Component]] = None
    // 向图中添加一个新节点（组件）
    def add(comp: Component): Unit = synchronized {
      require(
        !registry.isUidRegistered(comp.uid),
        s"Component UID ${comp.uid} already registered: ${comp.name()}")
      require(
        !registry.isClassRegistered(comp.getClass),
        s"Component class ${comp.getClass} already registered: ${comp.name()}")
      registry.register(comp)
      sortedComponents = None
    }

    // 在两个组件之间连线，声明依赖关系。
    def declareDependency(comp: Component, dependencyCompClass: Class[_ <: Component]): Unit =
      synchronized {
        require(registry.isUidRegistered(comp.uid))
        require(registry.isClassRegistered(comp.getClass))
        uidAndDependencyPairs += comp.uid -> dependencyCompClass
        sortedComponents = None
      }
    // 将扁平的依赖对转换为真正的图结构（Node 节点映射）
    private def newLookup(): Map[Int, Node] = {
      val uidToNodeLookup: mutable.Map[Int, Node] = mutable.Map()
      // 为每个 UID 创建一个 Node 对象。
      registry.allUids().foreach {
        uid =>
          require(!uidToNodeLookup.contains(uid))
          val n = new Node(uid)
          uidToNodeLookup(uid) = n
      }
      // 遍历所有依赖对，找到对应的 Node，设置 parents（父节点/被依赖者）和 children（子节点/依赖者）。
      uidAndDependencyPairs.foreach {
        case (uid, dependencyCompClass) =>
          require(
            registry.isClassRegistered(dependencyCompClass),
            s"Dependency class not registered yet: ${dependencyCompClass.getName}")
          val dependencyUid = registry.findByClass(dependencyCompClass).uid
          require(uid != dependencyUid)
          require(uidToNodeLookup.contains(uid))
          require(uidToNodeLookup.contains(dependencyUid))
          val n = uidToNodeLookup(uid)
          val r = uidToNodeLookup(dependencyUid)
          require(!n.parents.contains(r.uid))
          require(!r.children.contains(n.uid))
          n.parents(r.uid) = r
          r.children(n.uid) = n
      }

      uidToNodeLookup.toMap
    }
    // 执行拓扑排序，返回最终有效的组件序列。
    // 基于 Kahn 算法
    // 入度统计：统计每个节点有多少个 parents。
    // 寻找起点：将所有入度为 0 的节点（不依赖任何人的组件）放入 removalQueue 队列。
    // 循环剥离：
    // 从队列中取出一个节点（父），加入结果集。
    // 遍历该节点的 children（子），将它们的入度减 1。
    // 如果某个子的入度降为 0，说明其依赖已全部满足，将其入队。
    // 循环检测：如果处理完后还有节点入度不为 0，说明图中存在环路依赖，直接抛出 UnsupportedOperationException。
    // 兼容性过滤：
    // 定义 isRuntimeCompatible 递归检查。
    // 规则：一个组件要生效，必须满足“自身兼容”且“所有父组件也兼容”。
    // 将不兼容的组件剔除并记录警告日志。
    def sorted(): Seq[Component] = synchronized {
      if (sortedComponents.isDefined) {
        return sortedComponents.get
      }

      val lookup: Map[Int, Node] = newLookup()

      val sortedComponentsBuffer = mutable.Buffer[Component]()
      val uidToNumParents = mutable.Map[Int, Int]()
      uidToNumParents ++= lookup.map { case (uid, node) => uid -> node.parents.size }
      val removalQueue = mutable.Queue[Int]()

      // 1. Find out all nodes with zero parents then enqueue them.
      uidToNumParents.filter(_._2 == 0).foreach(kv => removalQueue.enqueue(kv._1))

      // 2. Loop to dequeue and remove nodes from the uid-to-num-parents map.
      while (removalQueue.nonEmpty) {
        val parentUid = removalQueue.dequeue()
        val node = lookup(parentUid)
        sortedComponentsBuffer += registry.findByUid(parentUid)
        node.children.keys.foreach {
          childUid =>
            uidToNumParents(childUid) = uidToNumParents(childUid) - 1
            val updatedNumParents = uidToNumParents(childUid)
            assert(updatedNumParents >= 0)
            if (updatedNumParents == 0) {
              removalQueue.enqueue(childUid)
            }
        }
      }

      // 3. If there are still outstanding nodes (those are with more non-zero parents) in the
      // uid-to-num-parents map, then it means at least one cycle is found. Report error if so.
      if (uidToNumParents.exists(_._2 != 0)) {
        val cycleNodes = uidToNumParents.filter(_._2 != 0).keys.map(registry.findByUid)
        val cycleNodeNames = cycleNodes.map(_.name()).mkString(", ")
        throw new UnsupportedOperationException(
          s"Cycle detected in the component graph: $cycleNodeNames")
      }

      // 4. Return the ordered components, with the incompatible ones excluded.
      def isRuntimeCompatible(component: Component): Boolean = {
        val parents = lookup(component.uid).parents.keys.map(registry.findByUid)
        component.isRuntimeCompatible && parents.forall(isRuntimeCompatible)
      }
      val (compatibleComponents, incompatibleComponents) =
        sortedComponentsBuffer.partition(isRuntimeCompatible)
      incompatibleComponents.foreach {
        component => logWarning(s"Excluding runtime-incompatible component: ${component.name()}.")
      }
      sortedComponents = Some(compatibleComponents.toSeq)
      sortedComponents.get
    }
  }

  private object Graph {
    class Node(val uid: Int) {
      val parents: mutable.Map[Int, Node] = mutable.Map()
      val children: mutable.Map[Int, Node] = mutable.Map()
    }
  }

  case class BuildInfo(name: String, branch: String, revision: String, revisionTime: String)
}
