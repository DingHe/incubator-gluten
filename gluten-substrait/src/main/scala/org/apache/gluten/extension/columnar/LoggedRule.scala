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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.metrics.GlutenTimeMetric

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.execution.SparkPlan

/** Since https://github.com/apache/incubator-gluten/pull/7606. */
// LoggedRule 类是 Apache Gluten 项目中一个非常实用的装饰器类（Decorator）。
// 它通过对原有的 Spark 优化规则进行包装，提供了透明的执行耗时统计和计划变化对比日志功能。
// 在 Spark SQL 的开发和调试中，理解每一条规则（Rule）对物理计划（SparkPlan）做了什么改变，以及每条规则执行了多久，对于性能调优和排查回退（Fallback）原因至关重要。
// 规则透明监控：它不改变原有规则的逻辑，只是在规则执行前后插入监控。
// 可视化对比：如果规则改变了计划（例如将行式算子替换成了 Gluten 的列式算子），它会以“左右对比（Side-by-Side）”的方式打印出计划树的变化。
// 性能分析：记录每条规则消耗的时间（毫秒），帮助定位优化器中的性能瓶颈。
class LoggedRule(delegate: Rule[SparkPlan]) extends Rule[SparkPlan] with Logging with LogLevelUtil {
  // 获取并保存被委派规则的名称。
  override val ruleName: String = delegate.ruleName
  // 构建用于打印的日志字符串。
  private def message(oldPlan: SparkPlan, newPlan: SparkPlan, millisTime: Long): String =
    if (!oldPlan.fastEquals(newPlan)) {
      s"""
         |=== Applying Rule $ruleName took $millisTime ms ===
         |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
    } else {
      s"Rule $ruleName has no effect, took $millisTime ms."
    }
  // 规则执行的入口，也是装饰逻辑发生的地方。
  override def apply(plan: SparkPlan): SparkPlan = {
    val (out, millisTime) = GlutenTimeMetric.recordMillisTime(delegate.apply(plan))
    logOnLevel(GlutenConfig.get.transformPlanLogLevel, message(plan, out, millisTime))
    out
  }
}
