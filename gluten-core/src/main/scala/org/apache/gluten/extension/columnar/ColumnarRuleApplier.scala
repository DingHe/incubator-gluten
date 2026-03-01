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
package org.apache.gluten.extension.columnar

import org.apache.gluten.extension.caller.CallerInfo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
// Gluten 内部物理计划转换流程的执行契约。它规定了如何将一个 SparkPlan（物理计划）按照预定的规则进行处理和转换
// 在 Gluten 中，物理计划的转换不是一蹴而就的，而是由 GlutenColumnarRule 调用一个“应用器”（Applier）来完成。
// 抽象执行逻辑：该接口屏蔽了底层转换的复杂性。无论内部是使用简单的顺序规则（Heuristic/Legacy 模式），还是复杂的代价模型搜索（RAS 模式），对于调用者来说，接口都是统一的。
// 物理转换核心：它是 Gluten 核心逻辑的“处理器”，负责接收原始的 Spark 物理计划，并输出优化后（通常是本地列式算子）的物理计划。
trait ColumnarRuleApplier {
  // 参数 plan：传入的原始 Spark 物理计划。
  // 参数 outputsColumnar：一个布尔值，指示当前的转换上下文是否期望输出列式数据（Columnar Batch）。
  // 返回值：返回转换后的 SparkPlan。
  def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan
}

object ColumnarRuleApplier {
  // 这是一个上下文载体类，封装了规则执行过程中所需的全部环境信息。在 GlutenInjector 中，很多规则生成器（Builders）都需要这个对象作为输入参数。
  class ColumnarRuleCall(
      // 当前的 Spark 会话。
      // 用于获取访问 Catalyst 器件、函数注册表或 Session 状态。
      val session: SparkSession,
      // 记录是谁触发了这次规则应用。这在调试、日志记录或多层嵌套调用时非常有用，可以帮助追踪计划转换的来源。
      val caller: CallerInfo,
      // 作用：传递外部对输出格式的要求。
      val outputsColumnar: Boolean) {
    // 作用：获取 Spark SQL 的配置参数（SQLConf）。
    val sqlConf = session.sessionState.conf
  }
}
