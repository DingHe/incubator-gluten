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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.{ColumnarRuleApplier, ColumnarRuleExecutor}
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by enumerating on
 * all the possibilities of executable Gluten plans, then choose the best plan among them.
 *
 * NOTE: We still have a bunch of heuristic rules in this implementation's rule list. Future work
 * will include removing them from the list then implementing them in EnumeratedTransform.
 */
// EnumeratedApplier 是 Apache Gluten 项目中负责执行 RAS（Relational Algebra Service，关系代数服务） 优化模式的列式规则应用器。
// 与基于固定顺序的 HeuristicApplier（启发式应用器）不同，EnumeratedApplier 的核心设计理念是穷举与代价评估。它通过 RAS 引擎探索物理计划的所有可能排列组合（即枚举），并根据代价模型（Cost Model）选出最优的执行路径。
// 该类的主要职责是将一组动态生成的规则应用到 Spark 物理计划上，以实现向 Gluten Native 计划的转换：
// RAS 模式的入口：它是 Gluten 进入“基于代价优化（CBO）”物理转换阶段的执行器。
// 枚举搜索：它利用 RAS 框架的能力，不仅仅是简单地替换算子，而是考虑多种可能的转换方案（例如：不同的 Join 实现或不同的数据分布方式）。
// 规则集成：它将一组由外部注入的规则（Rule）和包装器（Wrapper）整合在一起，形成一个完整的执行上下文。
// 平滑过渡：正如代码中的 NOTE 所言，目前该类依然保留了一些启发式规则，作为从传统模式向全 RAS 模式演进的过渡方案。
class EnumeratedApplier(
    session: SparkSession,
    // 规则构建器序列。
    // 这些构建器是函数式定义的。在执行阶段，它们会接收 ColumnarRuleCall 参数并生成具体的 Rule[SparkPlan]。这些规则中通常包含核心的 EnumeratedTransform（即 RAS 的核心转换逻辑）
    ruleBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    // 规则包装器序列。
    // 用于对生成的规则进行二次封装。例如，注入日志记录（Logging）、性能计数（Metrics）或权限检查等横切关注点逻辑。
    ruleWrappers: Seq[Rule[SparkPlan] => Rule[SparkPlan]])
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {

  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, CallerInfo.create(), outputsColumnar)
    val finalPlan = apply0(
      ruleBuilders
        // 将构建器转化为具体的规则实例。
        .map(b => b(call))
        // 使用 foldLeft 将所有的 ruleWrappers 依次套在每个规则之上
        .map(r => ruleWrappers.foldLeft(r) { case (r, wrapper) => wrapper(r) }),
      plan)
    finalPlan
  }
  // 实例化一个 ColumnarRuleExecutor。注意其第一个参数传入了 "ras"，这标识了当前处于 RAS 转换阶段。
  private def apply0(rules: Seq[Rule[SparkPlan]], plan: SparkPlan): SparkPlan =
    new ColumnarRuleExecutor("ras", rules).execute(plan)
}

object EnumeratedApplier {}
