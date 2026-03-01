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

import org.apache.spark.sql.SparkSessionExtensions

/** Injector used to inject extensible components into Spark and Gluten. */
// Injector 类是 Apache Gluten 插件系统的总入口和协调者。
// 它负责整合所有的注入逻辑，并将 Gluten 的自定义功能平滑地嵌入到 Apache Spark 的执行流程中。
// Injector 类在整个项目中扮演着“中央枢纽”的角色：
// 组合与聚合：它将 SparkInjector（负责向 Spark 注入规则）和 GlutenInjector（负责在 Gluten 内部注入规则）组合在一起，对外提供统一的注入接口。
// 生命周期管理：通过持有 InjectorControl，它确保了所有注入的组件都共享同一个动态开关控制逻辑，从而实现 Gluten 插件的“一键启用/禁用”。
// 扩展点对接：它是外部组件（如 Velox 或 ClickHouse 后端）与 Spark 引擎之间的媒介，所有后端特有的优化规则都要通过这个 Injector 实例进入系统。
class Injector(extensions: SparkSessionExtensions) {
  val control = new InjectorControl()
  // 专门负责处理 Spark 原生扩展点的注入器。
  val spark: SparkInjector = new SparkInjector(control, extensions)
  // 专门负责处理 Gluten 内部列式转换流的注入器。
  val gluten: GlutenInjector = new GlutenInjector(control)

  private[extension] def inject(): Unit = {
    // The regular Spark rules already injected with the `injectRules` of `RuleApi` directly.
    // Only inject the Spark columnar rule here.
    gluten.inject(extensions)
  }
}
