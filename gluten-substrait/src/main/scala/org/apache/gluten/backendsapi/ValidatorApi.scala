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

import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.substrait.`type`.TypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.plan.PlanNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

/**
 * Determine if a plan or expression can be accepted by the backend, or we fallback the execution to
 * vanilla Spark.
 */
// ValidatorApi 是 Apache Gluten 架构中的准入控制中心。它的核心任务是回答一个关键问题：“当前的这个算子、表达式或数据类型，Native 后端（如 Velox 或 ClickHouse）能否正确支持？”
// 如果校验失败，Gluten 会自动执行“回退（Fallback）”机制，让该部分逻辑返回到原生的 Spark（Vanilla Spark）中执行，从而保证查询的正确性。
// 能力边界检查：Native 后端并非 100% 覆盖 Spark 的所有功能。该类定义了后端必须实现的校验接口，用于识别不支持的场景。
// 确保结果一致性：如果某个表达式在 Native 端的实现与 Spark 标准行为不一致（例如精度处理不同），通过此 API 可以在规划阶段就将其拦截。
// 性能预判：在某些情况下，虽然 Native 能跑，但如果数据格式或压缩方式不适合向量化处理，也可以通过此接口建议回退。
// 失败原因追踪：它不仅返回能不能支持，还可以返回详细的失败原因（Failure Reason），这些信息会展示在 Spark UI 的 Gluten 标签页中，方便开发者调试。
trait ValidatorApi {

  /**
   * Validate expression for specific backend, including input type. If the expression isn't
   * implemented by the backend or it returns mismatched results with Vanilla Spark, it will fall
   * back to Vanilla Spark.
   *
   * @return
   *   true by default
   */
    // 对单个 Spark 表达式进行初步校验。
    // 检查后端是否实现了该表达式（通过其 Substrait 名称识别）。
    // 应用场景：例如，如果 Spark 调用了一个高级数学函数，但 Velox 后端尚未实现该函数，此方法应返回 false。
  def doExprValidate(substraitExprName: String, expr: Expression): Boolean = true

  /** Validate against Substrait plan node in native backend. */
  // 核心校验逻辑。针对生成的 Substrait 计划节点（PlanNode）调用 Native 库进行深度校验。
  // Gluten 会将一段逻辑尝试转化为 Substrait 计划，然后通过 JNI 传递给 Native 侧（如 Velox 的 doValidate）。
  def doNativeValidateWithFailureReason(plan: PlanNode): ValidationResult

  /** Validate expression in native backend. */
  // 在 Native 侧对具体表达式节点进行深度校验。
  // 应用场景：某些函数可能支持 Int 类型，但不支持 Decimal 类型。该方法通过检查 Substrait 表达式树和输入类型来判断 Native 引擎是否能处理。
  def doNativeValidateExpression(
      substraitContext: SubstraitContext,
      expression: ExpressionNode,
      inputTypeNode: TypeNode): Boolean =
    false

  /** Validate against Compression method, such as bzip2. */
  // 校验压缩格式是否支持并行切分。
  // 详细说明：在读取文件时，某些压缩格式（如 bzip2）在特定条件下是可切分的，而有些则不是。如果 Native 引擎对某种压缩格式的并行读取支持有局限，可以通过此方法拦截。
  def doCompressionSplittableValidate(compressionMethod: String): Boolean = false

  /**
   * Validate the input schema. Transformers like UnionExecTransformer that do not generate
   * Substrait plan need to validate the input schema and fall back if there are any unsupported
   * types.
   *
   * @return
   *   An option contain a validation failure reason, none means ok
   */
  // 对输入的数据类型（Schema）进行校验。
  // 这是针对不支持生成 Substrait 计划但仍需进行列式处理的算子（如 UnionExec）。
  def doSchemaValidate(schema: DataType): Option[String] = None

  /** Validate against ColumnarShuffleExchangeExec. */
  // 专门针对 Shuffle（数据重分布） 过程进行校验。
  def doColumnarShuffleExchangeExecValidate(
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String]
}
