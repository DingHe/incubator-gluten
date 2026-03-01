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
package org.apache.gluten.extension.injector

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.extension.GlutenColumnarRule
import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.cost.GlutenCostModel
import org.apache.gluten.extension.columnar.enumerated.{EnumeratedApplier, EnumeratedTransform}
import org.apache.gluten.extension.columnar.heuristic.{HeuristicApplier, HeuristicTransform}
import org.apache.gluten.ras.rule.RasRule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/** Injector used to inject query planner rules into Gluten. */
// GlutenInjector 是 Apache Gluten 插件系统的核心组成部分，
// 它专门负责向 Gluten 的**列式规则执行器（Columnar Rule Applier）**中注入自定义的转换逻辑。
// 与 SparkInjector（负责向原生 Spark 注入）不同，GlutenInjector 负责的是 Gluten 内部物理计划转换的组织工作。
// GlutenInjector 的主要作用是构建 Gluten 物理层转换的流水线：
// 多模式支持：它支持两种不同的查询优化模式：Legacy（启发式模式） 和 RAS（基于代价的递归搜索模式）。
// 规则分层管理：它将 Gluten 的物理转换逻辑细分为：预转换（Pre-Transform）、核心转换（Transform/RAS Rule）、回退策略（Fallback）和后置处理（Post/Final）。
// 桥接 Spark：它最终通过 inject 方法，将包装好的 GlutenColumnarRule 注册到 Spark 的 SparkSessionExtensions 中，作为列式转换的入口。
class GlutenInjector private[injector] (control: InjectorControl) {
  import GlutenInjector._
  // 启发式转换模式的注入器，适用于传统的、基于规则顺序的转换逻辑。
  val legacy: LegacyInjector = new LegacyInjector()
  // 更加高级的转换模式（RAS, Relational Algebra Service），支持基于代价模型（Cost Model）的最优路径搜索。
  val ras: RasInjector = new RasInjector()
  // 将 Gluten 的核心列式规则注入到 Spark 中。
  private[injector] def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(
      control.disabler().wrapColumnarRule(s => new GlutenColumnarRule(s, applier)))
  }
  // 根据配置决定使用哪种转换执行器。
  private def applier(session: SparkSession): ColumnarRuleApplier = {
    val conf = new GlutenCoreConfig(session.sessionState.conf)
    // 如果开启则返回 ras 创建的执行器，否则使用 legacy 模式。
    if (conf.enableRas) {
      return ras.createApplier(session)
    }
    legacy.createApplier(session)
  }
}

object GlutenInjector {
  // 启发式模式
  class LegacyInjector {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val transformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val fallbackPolicyBuilders =
      mutable.Buffer.empty[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]]
    private val postBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val finalBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val ruleWrappers = mutable.Buffer.empty[Rule[SparkPlan] => Rule[SparkPlan]]
    // 注入在核心转换之前的预处理规则（如某些元数据准备）
    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }
    // 注入核心的转换规则（如将 FileSourceScanExec 替换为 GlutenScan）。
    def injectTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      transformBuilders += builder
    }
    // 注入核心转换之后的微调规则。
    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }
    // 注入回退策略。当某个算子无法在 Native 执行时，由这些规则决定如何回退到原始 Spark 算子。
    def injectFallbackPolicy(builder: ColumnarRuleCall => SparkPlan => Rule[SparkPlan]): Unit = {
      fallbackPolicyBuilders += builder
    }
    // 注入流水线末端的规则，通常用于清理或最终格式化。
    def injectPost(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postBuilders += builder
    }

    def injectFinal(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      finalBuilders += builder
    }

    def injectRuleWrapper(wrapper: Rule[SparkPlan] => Rule[SparkPlan]): Unit = {
      ruleWrappers += wrapper
    }
    // 最终构建出一个 HeuristicApplier（启发式应用器），它会严格按照预定义的顺序执行上述所有规则。
    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new HeuristicApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createHeuristicTransform(c)) ++ postTransformBuilders).toSeq,
        fallbackPolicyBuilders.toSeq,
        postBuilders.toSeq,
        finalBuilders.toSeq,
        ruleWrappers.toSeq
      )
    }

    def createHeuristicTransform(call: ColumnarRuleCall): HeuristicTransform = {
      val all = transformBuilders.map(_(call))
      HeuristicTransform.withRules(all.toSeq)
    }
  }
  // RAS 模式不强求固定的规则执行顺序，而是通过代价模型来寻找最优解。
  class RasInjector extends Logging {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val rasRuleBuilders = mutable.Buffer.empty[ColumnarRuleCall => RasRule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val ruleWrappers = mutable.Buffer.empty[Rule[SparkPlan] => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }
    // 注入 RasRule。这些规则不是简单的 Plan 替换，而是告诉 RAS 引擎一个算子有哪些等价的表达形式。
    def injectRasRule(builder: ColumnarRuleCall => RasRule[SparkPlan]): Unit = {
      rasRuleBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    def injectRuleWrapper(wrapper: Rule[SparkPlan] => Rule[SparkPlan]): Unit = {
      ruleWrappers += wrapper
    }
    // 构建出一个 EnumeratedApplier（枚举应用器）。
    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new EnumeratedApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createEnumeratedTransform(c)) ++ postTransformBuilders).toSeq,
        ruleWrappers.toSeq)
    }
    // 这是 RAS 模式的核心。它会聚合所有的 RasRule，并根据配置加载对应的 GlutenCostModel（代价模型）。
    // 在运行时，它会遍历计划，计算各种可能组合的 Cost，选择总开销最小的路径（通常是 Native 算子覆盖率最高的路径）。
    def createEnumeratedTransform(call: ColumnarRuleCall): EnumeratedTransform = {
      // Build RAS rules.
      val rules = rasRuleBuilders.map(_(call))
      val costModel = GlutenCostModel.find(new GlutenCoreConfig(call.sqlConf).rasCostModel)
      // Create transform.
      EnumeratedTransform(costModel, rules.toSeq)
    }
  }
}
