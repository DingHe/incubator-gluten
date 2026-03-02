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
package org.apache.gluten.backendsapi

import org.apache.gluten.extension.injector.Injector
// 在 Apache Gluten 的架构中，RuleApi 是规则注入的定义规范。它定义了后端（Backend）如何将其特有的优化和转换规则挂载到 Spark 的执行引擎中。
// 它是 SubstraitBackend 依赖的核心 API 组件之一，负责解决“如何将 Spark 算子替换为 Native 算子”的问题。
// 规则分发中心：它作为后端与 Spark Injector 之间的中转站。具体的后端（如 Velox 或 ClickHouse）通过实现这个接口，来决定哪些自定义规则需要被“注册”到 Spark 中。
// 物理计划转换的起点：在 Gluten 中，从传统的行式 SparkPlan 转换到列式 Native 计划，本质上是靠一系列 Rule 完成的。RuleApi 正是这些规则的注入入口。
// 解耦后端差异：不同的后端可能有完全不同的优化策略。例如，Velox 后端可能需要注入特定的内存管理规则，而 ClickHouse 可能需要特定的 Join 转换规则。RuleApi 使得这些差异对核心框架透明。
trait RuleApi {
  // Injects all Spark query planner rules used by the Gluten backend.
  def injectRules(injector: Injector): Unit
}
