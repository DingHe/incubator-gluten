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

/** Injector used to inject query planner rules into Spark. */
// SparkInjector 是 Apache Gluten 项目中用于安全地向 Spark 注入扩展逻辑的代理类。它充当了原生 SparkSessionExtensions 与 Gluten 的 InjectorControl 之间的中间层。
// 在标准的 Spark 开发中，开发者直接通过 SparkSessionExtensions 注入规则。但 Gluten 引入了动态开关机制，因此需要 SparkInjector 来完成以下任务：
// 装饰者模式应用：它会自动将所有注入的规则（Rules）、策略（Strategies）、解析器（Parsers）和函数（Functions）使用 InjectorControl 提供的包装方法（如 wrapRule, wrapParser 等）进行封装。
// 集中化管理：它屏蔽了底层的“装饰/包装”细节。Gluten 的各个组件（Component）只需要调用 SparkInjector 的接口，就能确保自己注入的逻辑是受 disabler 开关控制的。
// 确保安全回退：通过这种注入方式，Gluten 确保了每一个扩展点都具备“动态禁用”的能力，从而在发生异常时能平稳回滚到 Spark 原生执行路径。
class SparkInjector private[injector] (
    control: InjectorControl, // 持有一个控制器实例，该实例包含了所有的禁用逻辑（Disablers）
    extensions: SparkSessionExtensions) { // Spark 原生的扩展句柄。
  // 这些方法几乎涵盖了 Spark SQL 执行流程的所有关键扩展点。
  // 注入 QueryStage 准备规则。
  // 详细说明：在 Adaptive Query Execution (AQE) 模式下，用于在划分 QueryStage 之前对物理计划进行处理。包装后，如果 Gluten 被禁用，该准备逻辑将不执行。
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    extensions.injectQueryStagePrepRule(control.disabler().wrapRule(builder))
  }
  // 注入 Analyzer 解析规则。
  // 用于将未解析的逻辑计划（Unresolved Logical Plan）转换为解析后的逻辑计划。常用于自定义的元数据解析逻辑。
  def injectResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectResolutionRule(control.disabler().wrapRule(builder))
  }
  // 注入 解析后置规则。
  // 详细说明：在 Spark 完成标准解析后执行的额外规则，通常用于处理某些特殊的注入逻辑，确保在优化器介入前计划已完全确定。
  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectPostHocResolutionRule(control.disabler().wrapRule(builder))
  }
  // 作用：注入 优化器规则（Optimizer Rule）。
  // 详细说明：这是最常用的扩展点，用于逻辑计划优化（如谓词下推、常量折叠等）。Gluten 通过此点注入特定的逻辑转换逻辑。
  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    extensions.injectOptimizerRule(control.disabler().wrapRule(builder))
  }
  // 作用：注入 物理计划策略（Strategy）。
  // 详细说明：将逻辑算子（LogicalPlan）映射为物理算子（SparkPlan）。通过 wrapStrategy 包装，如果禁用则返回 Nil，让 Spark 尝试原生策略。
  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    extensions.injectPlannerStrategy(control.disabler().wrapStrategy(builder))
  }
  // 作用：注入 SQL 解析器（Parser）。
  // 详细说明：替换 Spark 的 SQL 语法解析逻辑。使用 wrapParser 包装（通过动态代理实现），如果禁用则回退到 Spark 原生解析器。
  def injectParser(builder: ParserBuilder): Unit = {
    extensions.injectParser(control.disabler().wrapParser(builder))
  }
  // 作用：注入 自定义函数（UDF/Built-in）。
  // 详细说明：向 Spark 注册新的内置函数。使用 wrapFunction 包装，如果禁用，调用该函数会触发“不支持操作”的异常。
  def injectFunction(functionDescription: FunctionDescription): Unit = {
    extensions.injectFunction(control.disabler().wrapFunction(functionDescription))
  }
  // 作用：注入 CBO 之前的规则。
  // 详细说明：在基于开销的优化器（Cost-Based Optimizer）运行之前执行的优化规则。
  def injectPreCBORule(builder: RuleBuilder): Unit = {
    extensions.injectPreCBORule(control.disabler().wrapRule(builder))
  }
}
