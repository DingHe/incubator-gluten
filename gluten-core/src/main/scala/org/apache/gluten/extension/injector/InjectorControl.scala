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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.{SparkStrategy => Strategy}

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method}

import scala.collection.mutable
// InjectorControl 是 Apache Gluten 项目中负责动态控制扩展生效的核心类。
// 简单来说，Gluten 会向 Spark 注入很多自定义规则（如 SQL 优化规则、执行策略等）。
// InjectorControl 的作用就是给这些注入项加一个“开关”。如果满足某些条件（Disabler 为 true），这些注入的规则就会失效，回退到 Spark 默认行为。
// 在复杂的 Spark 运行环境中，我们有时需要根据特定场景禁用 Gluten 的某些部分。InjectorControl 通过**装饰器模式（Decorator Pattern）**封装了所有注入到 Spark 的扩展项：
// 动态控制：它允许在运行时根据 SparkSession 的配置或状态，决定是否应用某个注入规则。
// 组合禁用逻辑：它可以聚合多个 Disabler 条件，只要其中一个条件说“禁用”，该规则就不再执行。
// 无缝回退：当规则被禁用时，它确保 Spark 能够无缝回滚到原生逻辑（例如 Parser 会回退到旧解析器，Rule 会直接返回原 Plan）。
class InjectorControl private[injector] () {
  import InjectorControl._
  // 用于存储所有注册进来的禁用逻辑（Disabler）。
  private val disablerBuffer: mutable.ListBuffer[Disabler] =
    mutable.ListBuffer()
  // 最终生效的复合禁用器。它通过逻辑“或（OR）”将所有 buffer 中的禁用器组合在一起。
  private var combined: Disabler = (_: SparkSession) => false
  // 注册一个新的禁用条件。
  def disableOn(one: Disabler): Unit = synchronized {
    disablerBuffer += one
    // Update the combined disabler.
    val disablerList = disablerBuffer.toList
    // 重新生成 combined 函数。只要 disablerList 中任何一个 Disabler 返回 true，combined 就会返回 true。
    combined = s => disablerList.exists(_.disabled(s))
  }

  private[injector] def disabler(): Disabler = synchronized {
    combined
  }
}

object InjectorControl {
  // 开发者实现该方法来定义具体的禁用逻辑（例如：如果配置了 spark.gluten.enabled=false，则返回 true）。
  trait Disabler {
    // If true, the injected rule will be disabled.
    protected[injector] def disabled(session: SparkSession): Boolean
  }

  private object Disabler {
    // 通过隐式转换，为 Disabler 提供了各种 wrap（包装）方法。
    // 每一个 wrap 方法都代表一种 Spark 扩展点的装饰逻辑：
    implicit private[injector] class DisablerOps(disabler: Disabler) {
      // 实现了对 Spark SQL 优化规则（Rule）的**装饰器模式（Decorator Pattern）**封装。其核心目的是在不改变原规则逻辑的前提下，动态地注入一个“开关”检查。
      // 包装对象：Spark 的 Rule[TreeNode]（如逻辑/物理优化规则）
      // 作用：如果被禁用，直接返回原始 plan，不执行 Gluten 的 Rule。
      // [TreeType <: TreeNode[_]]: 这是一个泛型约束。Spark 的所有计划（逻辑计划 LogicalPlan 和物理计划 SparkPlan）都继承自 TreeNode。这保证了该方法可以处理任何类型的 Spark 规则。
      // ruleBuilder: 输入参数是一个函数。在 Spark 扩展中，规则通常是由 SparkSession 创建的，因此输入是 SparkSession => Rule
      def wrapRule[TreeType <: TreeNode[_]](
          ruleBuilder: SparkSession => Rule[TreeType]): SparkSession => Rule[TreeType] = session =>
        {
          // 利用当前的 session 创建出真正执行逻辑的 Gluten 原始规则实例（例如 GlutenFallbackRule）
          val rule = ruleBuilder(session)
          // 这个新对象将作为“代理”运行在 Spark 的优化器中，而原始规则被隐藏在它内部。
          new Rule[TreeType] with DisablerAware {
            // 将包装类的规则名称设置为与原始规则一致。
            override val ruleName: String = rule.ruleName
            // 这是 Spark 优化器的入口。每当 Spark 处理一个执行计划节点时，都会调用这个方法。
            override def apply(plan: TreeType): TreeType = {
              // 调用外部传入的禁用逻辑。
              if (disabler.disabled(session)) {
                // 如果判定为“禁用”状态（例如用户设置了 spark.gluten.enabled=false），则直接返回原始计划（plan），不做任何修改
                return plan
              }
              // 如果禁用检查为 false，创建的原始规则 rule 来处理计划。这是 Gluten 真正发挥性能优化作用的地方。
              rule(plan)
            }
          }
        }
      // 实现了对 Spark SQL 执行策略（Strategy） 的包装。Strategy 的作用是将 Spark 的逻辑计划（LogicalPlan）转换为一个或多个物理计划（SparkPlan）候选方案。
      def wrapStrategy(strategyBuilder: StrategyBuilder): StrategyBuilder = session => {
        // strategyBuilder: 一个函数，入参为 SparkSession，返回一个 SparkStrategy（这是 Spark 将逻辑计划 LogicalPlan 转换为物理计划 SparkPlan 的核心策略）
        // 使用传入的 session 立即构建出原始的 Gluten 物理策略实例（例如，将 Logical 算子转换为 Gluten 的 Native 算子）
        val strategy = strategyBuilder(session)
        // 通过 new Strategy 创建一个匿名类实例，并混入 DisablerAware 接口。这使得该策略能够被识别为受控的注入项。
        new Strategy with DisablerAware {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
            if (disabler.disabled(session)) {
              // 这是 Spark 策略的一个特殊约定。在 Spark 的策略模式中，如果一个策略无法（或被禁止）处理当前的逻辑计划，它应返回空列表（Nil）。
              return Nil
            }
            strategy(plan)
          }
        }
      }
      // 通过 Java 动态代理（Dynamic Proxy） 实现了对 Spark SQL 解析器（Parser）的动态开关控制。
      // parserBuilder: 这是一个函数，接收 SparkSession 和原有的解析器，返回一个新的解析器（通常是 Gluten 扩展后的 GlutenSqlParser）
      def wrapParser(parserBuilder: ParserBuilder): ParserBuilder = (session, parser) => {
        // 保存“旧的”解析器（即 Spark 原生的解析器）。
        val before = parser
        // 调用 builder 生成“新的”解析器（Gluten 的解析器）
        val after = parserBuilder(session, before)
        // Use dynamic proxy to get rid of 3.2 compatibility issues.
        // 为什么要用动态代理？（关键注释）
        // 背景：Spark 不同版本（如 3.2, 3.3, 3.4+）之间的 ParserInterface 接口方法签名可能会发生微调（增加或删减方法）。
        // 痛点：如果使用 Scala 的静态类继承，代码必须在编译时确定实现的每一个方法。如果 Spark 删掉或增加了一个方法，编译后的代码在运行时就会报 NoSuchMethodError。
        // 解决方案：动态代理不需要在代码中写死方法名，它在运行时拦截所有方法调用，从而完美兼容不同版本的 Spark 接口。
        java.lang.reflect.Proxy
          .newProxyInstance(
            classOf[ParserInterface].getClassLoader,
            Array(classOf[ParserInterface], classOf[DisablerAware]),
            new InvocationHandler { // 拦截器逻辑的核心，所有对解析器的调用都会进入这个处理器的 invoke 方法。
              override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
                try {
                  if (disabler.disabled(session)) {
                    // 回退逻辑：使用反射执行 before（原生解析器）的相同方法，传入相同的参数。
                    return method.invoke(before, args: _*)
                  }
                  // 正常逻辑：如果启用，执行 after（Gluten 解析器）的方法。
                  method.invoke(after, args: _*)
                } catch {
                  case e: InvocationTargetException =>
                    // Unwrap the ITE.
                    throw e.getCause
                }
              }
            }
          )
          .asInstanceOf[ParserInterface]
      }
      // 用于包装 自定义函数（UDF/Built-in Functions） 的逻辑。它的主要目的是防止在 Gluten 被禁用的情况下，用户仍然调用了那些仅由 Gluten 提供的特殊函数。
      def wrapFunction(functionDescription: FunctionDescription): FunctionDescription = {
        // 使用 Scala 的模式匹配将这三个组件解构出来，以便后续对 builder 进行包装。
        val (identifier, info, builder) = functionDescription
        val wrappedBuilder: FunctionBuilder = new FunctionBuilder with DisablerAware {
          override def apply(children: Seq[Expression]): Expression = {
            if (
              // 与 Rule 或 Strategy 不同，Function Builder 触发时可能没有直接传递 Session 对象，因此这里通过 getActiveSession 尝试从线程上下文中获取当前的 Spark Session。
              // 如果 Gluten 当前被禁用，进入拦截逻辑。
              disabler.disabled(SparkSession.getActiveSession.getOrElse(
                throw new IllegalStateException("Active Spark session not found")))
            ) {
              throw new UnsupportedOperationException(
                s"Function ${info.getName} is not callable as Gluten is disabled")
            }
            builder(children)
          }
        }
        (identifier, info, wrappedBuilder)
      }
      // 在 Apache Spark 中，ColumnarRule 是一个特殊的扩展点，它允许开发者在“行转列（RowToColumnar）”或“列转行（ColumnarToRow）”的转换前后插入自定义的物理计划转换逻辑。
      // Gluten 正是利用这个钩子将标准的 Spark 算子替换为本地引擎的列式算子。
      // columnarRuleBuilder: 一个函数，接收 SparkSession 并返回一个 ColumnarRule 对象。
      def wrapColumnarRule(columnarRuleBuilder: ColumnarRuleBuilder): ColumnarRuleBuilder =
        session => {
          // 实例化 Gluten 原始的列式处理规则（例如负责将普通 Spark 算子转换为本地算子的物理规则）
          val columnarRule = columnarRuleBuilder(session)
          // 创建包装后的 ColumnarRule
          new ColumnarRule with DisablerAware {
            // 作用阶段：在 Spark 插入 RowToColumnar 算子之前执行。
            override val preColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  // 禁用检查：如果 disabler.disabled(session) 为真，直接 return plan。
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.preColumnarTransitions.apply(plan)
                }
              }
            }
            // 列式转换后规则
            // 作用阶段：在 Spark 插入 ColumnarToRow 算子之后执行。
            override val postColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.postColumnarTransitions.apply(plan)
                }
              }
            }
          }
        }
    }
  }

  /**
   * The entity (could be a rule, a parser, cost evaluator) that is dynamically injected to Spark,
   * whose effectivity is under the control by a disabler.
   */
  trait DisablerAware
}
