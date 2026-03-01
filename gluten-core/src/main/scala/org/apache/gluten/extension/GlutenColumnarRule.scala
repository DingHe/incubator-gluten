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
package org.apache.gluten.extension

import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.transition.Transitions
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

// GlutenColumnarRule 是 Apache Gluten 与 Spark 物理执行计划对接的核心桥梁。它继承自 Spark 的 ColumnarRule，负责将 Spark 原生的行式物理计划转换为 Gluten 的列式（Native）执行计划。
// 在 Spark SQL 的物理计划执行过程中，ColumnarRule 提供了两个切入点：preColumnarTransitions 和 postColumnarTransitions。
// GlutenColumnarRule 的核心作用是：
// 上下文感知：通过一种巧妙的“打标签”机制，探测当前查询计划是否需要输出列式数据（outputsColumnar）。
// 转换触发：作为 Gluten 所有物理规则的总入口。它不直接执行转换，而是初始化 ColumnarRuleApplier（如你之前看到的 HeuristicApplier 或 EnumeratedApplier）来处理计划。
// 算子替换：将 SparkPlan 树中的原生算子（如 FileSourceScanExec）替换为 Gluten 对应的 Native 算子。
object GlutenColumnarRule {
  // Utilities to infer columnar rule's caller's property:
  // ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
  // 一个不执行任何逻辑的包装节点。
  // 作用：作为一个标记，告诉后续步骤：“这个计划在进入 Gluten 规则之前，Spark 期望它是**行式（Row）**输出的”。
  private case class DummyRowOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }
  // 作用：作为一个标记，告诉后续步骤：“这个计划在进入 Gluten 规则之前，Spark 期望它是**列式（Columnar）**输出的”。
  private case class DummyColumnarOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }
}

case class GlutenColumnarRule(
    session: SparkSession,
    applierBuilder: SparkSession => ColumnarRuleApplier) // 根据当前的 Session 动态构建 ColumnarRuleApplier。这决定了 Gluten 是运行在启发式模式还是 RAS 模式。
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  import GlutenColumnarRule._

  /**
   * Note: Do not implement this API. We basically inject all of Gluten's physical rules through
   * `postColumnarTransitions`.
   *
   * See: https://github.com/oap-project/gluten/pull/4790
   */
    // 执行时机：在 Spark 插入行转列（RowToColumnar）算子之前调用。
  final override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // To infer caller's property: ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
    // 它检查输入 plan 是否支持列式输出。
    // 埋下伏笔。它利用 Spark 自动插入转型算子的特性，来探测最终执行环境对数据格式的要求。
    if (plan.supportsColumnar) {
      DummyColumnarOutputExec(plan)
    } else {
      DummyRowOutputExec(plan)
    }
  }
  // 执行时机：在 Spark 插入行转列/列转行算子之后调用。这是 Gluten 真正干活的地方。
  // 目的是通过逆向推导，搞清楚 Spark 最终到底想要什么格式的数据（行式还是列式），从而决定 Gluten 该如何处理计划。
  // 这一段逻辑通过观察 preColumnarTransitions 阶段埋下的“鱼饵”（Dummy 节点）被 Spark 怎么处理了，来推断 outputsColumnar（最终是否需要列式输出）。
  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    // 通过模式匹配（plan match），识别出在 pre 阶段埋下的 Dummy 标签。
    val (originalPlan, outputsColumnar) = plan match {
      // 我们在 pre 阶段放了一个标记不支持列式的 DummyRow。如果现在收到的还是它，说明 Spark 觉得这里本来就该输出**行式（Row）**数据，且没做额外处理。
      case DummyRowOutputExec(child) =>
        (child, false)
      // Spark 在我们的 DummyRow 之上套了一个 RowToColumnar。这说明虽然我们自称输出行，但下游（Parent 节点）其实需要**列式（Columnar）**数据。
      // RowToColumnarExec是spark的规则
      case RowToColumnarExec(DummyRowOutputExec(child)) =>
        (child, true)
      // 我们在 pre 阶段放了标记支持列式的 DummyColumnar。Spark 直接接受了，说明下游确实需要列式数据。
      case DummyColumnarOutputExec(child) =>
        (child, true)
      // Spark 把列式强转成了行式
      // Spark 在我们的 DummyColumnar 之上套了一个 ColumnarToRow。这说明虽然我们能输出列，但下游节点其实只想要行式数据。
      case ColumnarToRowExec(DummyColumnarOutputExec(child)) =>
        (child, false)
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave an issue at" +
            " https://github.com/apache/incubator-gluten.")
    }
    // 在正式开始 Gluten 转换前，先根据推导出的 outputsColumnar 要求，在原始计划的顶部插入必要的行转列或列转行算子，使其成为一个完整的、符合 Spark 标准的计划（Vanilla Plan）。
    val vanillaPlan = Transitions.insert(originalPlan, outputsColumnar)
    // 调用 GlutenInjector 中注入的构建器。
    val applier = applierBuilder.apply(session)
    // 将准备好的原生计划交给 Applier 处理。
    val out = applier.apply(vanillaPlan, outputsColumnar)
    out
  }
}
