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
package org.apache.gluten.substrait

import java.lang.{Long => JLong}
import java.security.InvalidParameterException
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
// 连接参数
case class JoinParams() {
  // Whether preProjection is needed in streamed side.
  // 流侧（左表）在 Join 前是否需要做字段投影（计算表达式）。
  var streamPreProjectionNeeded = false

  // Whether preProjection is needed in build side.
  // 构建侧（右表/广播表）在 Join 前是否需要投影。
  var buildPreProjectionNeeded = false

  // Whether postProjection is needed after Join.
  // Join 完成后是否需要对结果进行字段修剪或投影。
  var postProjectionNeeded = true

  // Whether is BHJ
  // 是否为广播哈希连接（Broadcast Hash Join）。
  var isBHJ = false

  // Whether the join is with condition
  // Join 是否带有复杂的非等值过滤条件。
  var isWithCondition = false
}
// 聚合参数
case class AggregationParams() {
  // Whether rowConstruction is needed.
  // 是否需要将聚合结果重新构造为行格式。
  var rowConstructionNeeded = false

  // Whether extraction from intermediate struct is needed.
  // 是否需要从中间状态的结构体中提取最终聚合值。
  var extractionNeeded = false
}
// 在 Apache Gluten 项目中，SubstraitContext 是计划转换（Plan Conversion）阶段的状态管理器。
// 它像一个“账本”，记录了在将 Spark 算子树转换为 Substrait 计划的过程中所需的所有全局信息、映射关系和性能参数。
// 在将 Spark 的物理计划（SparkPlan）翻译为 Substrait 协议的过程中，存在几个核心挑战：
// 符号化与寻址：Substrait 使用整数 ID（Anchors）来引用函数。Context 负责统一分配这些 ID。
// 算子追踪：一个 Spark 算子可能被拆分为多个 Substrait Rel（算子节点）。Context 负责建立这种“一对多”的跟踪关系，便于调试和计划校验。
// 参数透传：在转换深层表达式时，某些特定的物理执行参数（如 Join 是否需要预投影）需要跨类传递。Context 提供了这样一个全局存储空间。
// 在了解 SubstraitContext 之前，需要先看它持有的两个重要数据结构，它们用于指导 Native 端如何优化物理执行：
class SubstraitContext extends Serializable {
  // A map stores the relationship between function name and function id.
  // 存储 函数名 -> 函数ID 的映射。确保相同的函数（如 add）在整个 Substrait 计划中使用同一个 ID。
  private val functionMap = new JHashMap[String, JLong]()

  // A map stores the relationship between Spark operator id and its respective Substrait Rel ids.
  // 存储 Spark算子ID -> Substrait Rel ID列表 的映射。用于追踪转换后的血缘关系。
  private val operatorToRelsMap: JMap[JLong, JList[JLong]] = new JHashMap[JLong, JList[JLong]]()

  // Only for debug conveniently
  // 存储 Spark算子ID -> 算子名称（如 Filter, Project）。纯粹用于调试时可读。
  private val operatorToPlanNameMap = new JHashMap[JLong, String]()

  // A map stores the relationship between join operator id and its param.
  // 分别存储 Join 和 Agg 算子的物理执行细节。
  private val joinParamsMap = new JHashMap[JLong, JoinParams]()

  // A map stores the relationship between aggregation operator id and its param.
  private val aggregationParamsMap = new JHashMap[JLong, AggregationParams]()
  // 计数器。用于标识 Native 端输入迭代器的序号。
  private var iteratorIndex: JLong = 0L
  // 计数器。为每个转换中的 Spark 算子分配唯一的内部自增 ID。
  private var operatorId: JLong = 0L
  // 计数器。为生成的每个 Substrait Rel 节点分配唯一的自增 ID。
  private var relId: JLong = 0L

  def registerFunction(funcName: String): JLong = {
    if (!functionMap.containsKey(funcName)) {
      val newFunctionId: JLong = functionMap.size.toLong
      functionMap.put(funcName, newFunctionId)
      newFunctionId
    } else {
      functionMap.get(funcName)
    }
  }
  // 注册一个标量或聚合函数。
  def registeredFunction: JHashMap[String, JLong] = functionMap

  def nextIteratorIndex: JLong = {
    val id = this.iteratorIndex
    this.iteratorIndex += 1
    id
  }

  def currentIteratorIndex: JLong = {
    assert(iteratorIndex > 0)
    this.iteratorIndex - 1
  }

  /**
   * Register a rel to certain operator id.
   * @param operatorId
   *   operator id
   */
  def registerRelToOperator(operatorId: JLong): Unit = {
    if (operatorToRelsMap.containsKey(operatorId)) {
      val rels = operatorToRelsMap.get(operatorId)
      rels.add(relId)
    } else {
      val rels = new JArrayList[JLong]()
      rels.add(relId)
      operatorToRelsMap.put(operatorId, rels)
    }
    relId += 1
  }

  /** Add the relId and register to operator later */
  def nextRelId(): JLong = {
    val id = this.relId
    this.relId += 1
    id
  }

  /**
   * Return the registered map.
   * @return
   */
  def registeredRelMap: JMap[JLong, JList[JLong]] = operatorToRelsMap

  /**
   * Register the join params to certain operator id.
   * @param operatorId
   *   operator id
   * @param param
   *   join params
   */
  def registerJoinParam(operatorId: JLong, param: JoinParams): Unit = {
    if (joinParamsMap.containsKey(operatorId)) {
      throw new InvalidParameterException("Join param has already been registered.")
    } else {
      joinParamsMap.put(operatorId, param)
    }
  }

  /**
   * return the registered map
   * @return
   */
  def registeredJoinParams: JHashMap[JLong, JoinParams] = joinParamsMap

  /**
   * Register the aggregation params to certain operator id.
   * @param operatorId
   *   operator id
   * @param param
   *   aggregation params
   */
  def registerAggregationParam(operatorId: JLong, param: AggregationParams): Unit = {
    if (aggregationParamsMap.containsKey(operatorId)) {
      throw new InvalidParameterException("Aggregation param has already been registered.")
    } else {
      aggregationParamsMap.put(operatorId, param)
    }
  }

  /**
   * return the registered map
   * @return
   */
  def registeredAggregationParams: JHashMap[JLong, AggregationParams] = aggregationParamsMap

  def nextOperatorId(planName: String): JLong = {
    val id = this.operatorId
    operatorToPlanNameMap.put(id, planName)
    this.operatorId += 1
    id
  }

  /** Only for debug the plan id and plan name in `operatorToRelsMap` */
  def getOperatorToPlanNameMap: JHashMap[JLong, String] = operatorToPlanNameMap
}
