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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan

/** Since https://github.com/apache/incubator-gluten/pull/6143. */
// 在 Apache Gluten 项目中，GlutenCost 是对 Spark AQE 成本比较机制的底层重写。
// 它的核心逻辑非常精妙：在逻辑等价的情况下，通过比较计划的 ID，人为地让“更新”生成的计划（通常是 Gluten 转换后的计划）胜出。
class GlutenCost(val eval: CostEvaluator, val plan: SparkPlan) extends Cost {
  override def compare(that: Cost): Int = that match {
    case that: GlutenCost if plan eq that.plan =>
      0
    case that: GlutenCost if plan == that.plan =>
      // Plans are identical. Considers the newer one as having lower cost.
      -(plan.id - that.plan.id)
    case that: GlutenCost =>
      // Plans are different. Use the delegated cost evaluator.
      assert(eval == that.eval)
      eval.evaluateCost(plan).compare(eval.evaluateCost(that.plan))
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }

  override def hashCode(): Int = throw new UnsupportedOperationException()

  override def equals(obj: Any): Boolean = obj match {
    case that: Cost => compare(that) == 0
    case _ => false
  }
}
