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
package org.apache.gluten.expression

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

// ==== Expression transformer basic interface start ====
// 在 Apache Gluten 项目中，ExpressionTransformer 是一个 Trait（特征/接口），它是将 Spark 表达式转换为 Substrait 表达式的核心抽象。
// 其核心作用是定义 Spark 表达式向 Substrait 表达式转化的“翻译策略”。
// 在 Spark 的物理计划中，表达式是以 Catalyst Expression 树的形式存在的（例如 Add(Literal(1), AttributeReference(a))）。Gluten 的任务是将这些树结构映射到 Substrait 的 ExpressionNode 树。
// ExpressionTransformer 作为一个中间层：
// 解耦：它将复杂的转换逻辑从巨大的 ExpressionConverter 中抽离出来，每个具体的表达式（如加法、减法、函数调用）都有自己的 Transformer 实现。
// 递归构建：它利用树形结构递归地调用子节点的转换逻辑，最终构建出完整的 Substrait 表达式。
trait ExpressionTransformer {
  // 定义该表达式在 Substrait 协议中的规范名称。
  // Substrait 使用特定的字符串来标识函数（例如加法通常是 add，字符串截取是 substring）。子类需要重写此属性，提供后端（如 Velox）能够识别的函数名。
  def substraitExprName: String
  // 存储该表达式的子转换器列表。
  // 对应 Spark 表达式中的 children。通过持有子转换器，可以实现自顶向下的递归转换。
  def children: Seq[ExpressionTransformer]
  // 保存原始的 Spark 表达式对象。
  // 在转换过程中，有时需要访问原始表达式的元数据（如 AttributeId、Literal 的具体值等），该属性提供了对原始 Spark 对象的引用。
  def original: Expression
  // 获取表达式的输出数据类型。
  // 默认从 original.dataType 获取。这决定了转换后的 Substrait 节点在内存中的数据格式（如 I32, FP64 等）。
  def dataType: DataType = original.dataType
  // 标识该表达式的结果是否可能为 Null。
  // 直接继承自 Spark 的属性，用于在 Substrait TypeNode 中设置 nullable 标记。
  def nullable: Boolean = original.nullable
  // 最核心的转换逻辑入口，负责将当前的 Transformer 实例变成 Substrait 的 ExpressionNode
  def doTransform(context: SubstraitContext): ExpressionNode = {
    // TODO: the funcName seems can be simplified to `substraitExprName`
    val funcName: String =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = context.registerFunction(funcName)
    val childNodes = children.map(_.doTransform(context)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

trait LeafExpressionTransformer extends ExpressionTransformer {
  final override def children: Seq[ExpressionTransformer] = Nil
}

trait UnaryExpressionTransformer extends ExpressionTransformer {
  def child: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = child :: Nil
}

trait BinaryExpressionTransformer extends ExpressionTransformer {
  def left: ExpressionTransformer
  def right: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = left :: right :: Nil
}

// ==== Expression transformer basic interface end ====

case class GenericExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer

object GenericExpressionTransformer {
  def apply(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): GenericExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child :: Nil, original)
  }
}

case class LiteralTransformer(original: Literal) extends LeafExpressionTransformer {
  override def substraitExprName: String = "literal"
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    ExpressionBuilder.makeLiteral(original.value, original.dataType, original.nullable)
  }
}
object LiteralTransformer {
  def apply(v: Any): LiteralTransformer = {
    LiteralTransformer(Literal(v))
  }
}
