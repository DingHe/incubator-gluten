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
package org.apache.spark.sql.execution.adaptive

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.{SparkVersionUtil, Utils}

/**
 * This [[CostEvaluator]] is to force use the new physical plan when cost is equal.
 *
 * Since https://github.com/apache/incubator-gluten/pull/6143.
 */
// 在 Apache Gluten 项目中，GlutenCostEvaluator 是一个对 Spark 原生成本评估机制进行“增强”和“拦截”的类。
// 它在 AQE（自适应查询执行）阶段起着决定性的作用，确保 Gluten 的原生执行计划在与 Spark 原生计划竞争时能够获得“合理的偏袒”。
// 这个类的核心作用是：在成本相等的情况下，强制 Spark 优先选择 Gluten 的物理计划。
// 在标准的 Spark AQE 中，如果两个物理计划的成本（Cost）完全相同，Spark 通常会保持原样或随机选择。但在 Gluten 场景下，我们通常希望即便成本看起来一样（例如 Shuffle 数量相同），也应该优先执行 Gluten 的原生（Native）算子，因为原生后端（如 Velox）在计算效率和内存管理上通常优于 JVM。
case class GlutenCostEvaluator() extends CostEvaluator with SQLConfHelper {

  // 原生评估器代理
  // 内部持有的 Spark 原生 SimpleCostEvaluator 实例。由于不同版本的 Spark 构造该类的方式不同，Gluten 在这里使用了反射技术来保持兼容性
  private val vanillaCostEvaluator: CostEvaluator = {
    if (SparkVersionUtil.lteSpark32) {
      val clazz = Utils.classForName("org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator$")
      clazz.getDeclaredField("MODULE$").get(null).asInstanceOf[CostEvaluator]
    } else {
      val forceOptimizeSkewedJoin =
        conf.getConfString("spark.sql.adaptive.forceOptimizeSkewedJoin").toBoolean
      val clazz = Utils.classForName("org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator")
      val ctor = clazz.getConstructor(classOf[Boolean])
      ctor.newInstance(forceOptimizeSkewedJoin.asInstanceOf[Object]).asInstanceOf[CostEvaluator]
    }
  }

  override def evaluateCost(plan: SparkPlan): Cost = {
    // 检查 Gluten 是否开启：通过 GlutenCoreConfig.get.enableGluten 判断
    // 装饰器模式 (Decorator Pattern)：它保留了原生评估器的功能，但在其基础上增加了 Gluten 的特殊逻辑。
    if (GlutenCoreConfig.get.enableGluten) {
      new GlutenCost(vanillaCostEvaluator, plan)
    } else {
      vanillaCostEvaluator.evaluateCost(plan)
    }
  }
}
