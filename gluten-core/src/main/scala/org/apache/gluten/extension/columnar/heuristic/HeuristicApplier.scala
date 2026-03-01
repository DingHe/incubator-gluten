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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.{ColumnarRuleApplier, ColumnarRuleExecutor}
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by heuristically
 * applying columnar rules in fixed order.
 */
// HeuristicApplier 是 Apache Gluten 中负责执行启发式（Heuristic）物理计划转换的核心类。
// 与 RAS（基于代价搜索）模式不同，它按照预定义的固定顺序应用一系列规则，将 Spark 的原生物理计划（Vanilla Plan）逐步转化为 Gluten 的列式执行计划（Native Plan）。
// 它的作用可以类比为一个“物理计划转换流水线”：
// 有序转换：它接收一组规则生成器，按照“转换 -> 检查回退 -> 后置处理 -> 最终清理”的严谨顺序处理物理计划。
// 策略执行：它不仅负责算子的替换（如 Row 转 Columnar），还负责处理“回退（Fallback）”逻辑。如果某个算子在 Native 端不支持，它会确保该部分能够平滑地切回到 Spark 原生执行。
// 确定性：启发式意味着转换路径是确定的，不涉及复杂的代价评估，执行效率高。
class HeuristicApplier(
    session: SparkSession, // 当前会话，用于提供配置和元数据环境。
    transformBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]], // 核心转换规则生成器。负责初步将 Spark 算子替换为 Gluten 算子。
    fallbackPolicyBuilders: Seq[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]], // 回退策略生成器。负责检查 transform 后的计划是否合法，若不合法则打上回退标记。
    postBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]], // 后置处理规则生成器。仅对未回退的计划生效，用于优化列式路径（如添加过渡算子）。
    finalBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]], // 最终规则生成器。无论计划是否回退都会执行，做最后的格式修正。
    ruleWrappers: Seq[Rule[SparkPlan] => Rule[SparkPlan]]) // 规则包装器。用于在执行规则前对其进行修饰（如增加日志记录或时间统计）。
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {
  // 接口入口方法。
  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, CallerInfo.create(), outputsColumnar)
    makeRule(call).apply(plan)
  }
  // 核心流水线逻辑
  // 定义了物理计划处理的生命周期。
  private def makeRule(call: ColumnarRuleCall): Rule[SparkPlan] = {
    originalPlan =>
      val suggestedPlan = transformPlan("transform", transformRules(call), originalPlan)
      val finalPlan = transformPlan(
        "fallback",
        fallbackPolicies(call).map(_(originalPlan)),
        suggestedPlan) match {
        case FallbackNode(fallbackPlan) =>
          // we should use vanilla c2r rather than native c2r,
          // and there should be no `GlutenPlan` anymore,
          // so skip the `postRules()`.
          fallbackPlan
        case plan =>
          transformPlan("post", postRules(call), plan)
      }
      transformPlan("final", finalRules(call), finalPlan)
  }
  // 规则执行的底层驱动。
  private def transformPlan(
      phase: String,
      rules: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    // 使用 ruleWrappers 包装所有传入的 rules。
    // 这段代码的意思是不断取出ruleWrappers中的函数，然后rules中的每个元素使用函数处理
    val wrappedRules = ruleWrappers.foldLeft(rules) {
      case (rules, wrapper) =>
        rules.map(wrapper)
    }
    // 实例化一个 ColumnarRuleExecutor（这是一个类似 Spark RuleExecutor 的组件）。
    // 在该阶段名下执行这些规则并返回结果。
    new ColumnarRuleExecutor(phase, wrappedRules).execute(plan)
  }

  /**
   * Rules to let planner create a suggested Gluten plan being sent to `fallbackPolicies` in which
   * the plan will be breakdown and decided to be fallen back or not.
   */
  private def transformRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    transformBuilders.map(b => b.apply(call))
  }

  /**
   * Rules to add wrapper `FallbackNode`s on top of the input plan, as hints to make planner fall
   * back the whole input plan to the original vanilla Spark plan.
   */
  private def fallbackPolicies(call: ColumnarRuleCall): Seq[SparkPlan => Rule[SparkPlan]] = {
    fallbackPolicyBuilders.map(b => b.apply(call))
  }

  /**
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    postBuilders.map(b => b.apply(call))
  }

  /*
   * Rules consistently applying to all input plans after all other rules have been applied, despite
   * whether the input plan is fallen back or not.
   */
  private def finalRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    finalBuilders.map(b => b.apply(call))
  }
}

object HeuristicApplier {}
