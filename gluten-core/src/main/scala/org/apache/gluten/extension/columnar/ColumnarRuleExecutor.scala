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

import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.SparkPlan

// ColumnarRuleExecutor 是 Apache Gluten 对 Spark Catalyst 引擎中 RuleExecutor 的一个轻量级实现。
// 它的核心任务是将一组特定的物理计划规则（Rules）打包成一个可执行的“批次”（Batch），并在特定的执行阶段（Phase）对 SparkPlan 进行转换。
// 在 Spark 中，所有的计划优化（逻辑或物理）都是通过 RuleExecutor 驱动的。ColumnarRuleExecutor 的作用如下：
// 规则容器：它将 HeuristicApplier 传进来的分散规则组合成一个逻辑单元。
// 阶段化执行：通过 phase 参数，它可以区分当前是在执行 transform（转换）、fallback（回退）还是 post（后置处理）阶段。
// 标准集成：它继承了 Spark 原生的 RuleExecutor[SparkPlan]，这意味着它复用了 Spark 稳定且经过测试的规则执行逻辑（如遍历树结构、应用规则等）。
// phase: String: 标识当前执行的阶段名称。
// rules: Seq[Rule[SparkPlan]]: 一组待执行的物理计划转换规则。
class ColumnarRuleExecutor(phase: String, rules: Seq[Rule[SparkPlan]])
  extends RuleExecutor[SparkPlan] {
  // 定义执行批次。
  private val batch: Batch = Batch(s"Columnar (Phase [$phase])", Once, rules: _*)

  // TODO: Remove this exclusion then manage to pass Spark's idempotence check.
  // 排除幂等性检查。
  // 这是一个关键的 Hack（临时方案）。Spark 的 RuleExecutor 默认会检查 Once 类型的 Batch 是否具有“幂等性”（即运行一次和运行多次的结果应该一样）。
  override protected val excludedOnceBatches: Set[String] = Set(batch.name)
  // 向父类 RuleExecutor 提供待运行的批次列表。
  override protected def batches: Seq[Batch] = Seq(batch)
}
