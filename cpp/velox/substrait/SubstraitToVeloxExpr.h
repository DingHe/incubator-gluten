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

#pragma once

#include "SubstraitParser.h"
#include "velox/core/Expressions.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten {

/// This class is used to convert Substrait representations to Velox
/// expressions.
// SubstraitVeloxExprConverter 是 Apache Gluten 项目中负责表达式层转换的核心类。
// 如果说 SubstraitToVeloxPlanConverter 是负责搭建查询计划的“骨架”，那么这个类就是负责填充骨架中的“血肉”——即具体的计算逻辑。
// 它将 Substrait 定义的通用表达式（如加减乘除、函数调用、常量等）翻译成 Velox 能够理解并执行的 TypedExpr 对象。
class SubstraitVeloxExprConverter {
 public:
  /// subParser: A Substrait parser used to convert Substrait representations
  /// into recognizable representations. functionMap: A pre-constructed map
  /// storing the relations between the function id and the function name.
  explicit SubstraitVeloxExprConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<uint64_t, std::string>& functionMap)
      : pool_(pool), functionMap_(functionMap) {}

  /// Stores the variant and its type.
  struct TypedVariant {
    variant veloxVariant;
    TypePtr variantType;
  };

  /// Convert Substrait Field into Velox Field Expression.
  static std::shared_ptr<const core::FieldAccessTypedExpr> toVeloxExpr(
      const ::substrait::Expression::FieldReference& substraitField,
      const RowTypePtr& inputType);

  /// Convert Substrait ScalarFunction into Velox Expression.
  core::TypedExprPtr toVeloxExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

  /// Convert Substrait SingularOrList into Velox Expression.
  core::TypedExprPtr toVeloxExpr(
      const ::substrait::Expression::SingularOrList& singularOrList,
      const RowTypePtr& inputType);

  /// Convert Substrait CastExpression to Velox Expression.
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression::Cast& castExpr, const RowTypePtr& inputType);

  /// Create expression for extract.
  // 作用：专门处理日期时间提取函数（如 extract(year from date)）。
  static core::TypedExprPtr toExtractExpr(const std::vector<core::TypedExprPtr>& params, const TypePtr& outputType);

  /// Used to convert Substrait Literal into Velox Expression.
  std::shared_ptr<const core::ConstantTypedExpr> toVeloxExpr(const ::substrait::Expression::Literal& substraitLit);

  /// Convert Substrait Expression into Velox Expression.
  // 通用的转换入口。
  // 逻辑：根据 Expression 的具体类型（如 Literal, ScalarFunction, FieldReference 等），分发给相应的专门处理方法。
  // RowTypePtr 参数提供了当前上下文的输入列信息。
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression& substraitExpr, const RowTypePtr& inputType);

  /// Convert Substrait IfThen into switch or if expression.
  core::TypedExprPtr toVeloxExpr(const ::substrait::Expression::IfThen& substraitIfThen, const RowTypePtr& inputType);

  /// Wrap a constant vector from literals with an array vector inside to create
  /// the constant expression.
  // 作用：将一组常量字面量封装成一个常量向量表达式，常用于处理数组常量。
  std::shared_ptr<const core::ConstantTypedExpr> literalsToConstantExpr(
      const std::vector<::substrait::Expression::Literal>& literals);

  /// Create expression for lambda.
  // 作用：处理 Lambda 表达式（如 transform(array, x -> x + 1)），将 Substrait 的匿名函数转换为 Velox 的 Lambda 结构。
  std::shared_ptr<const core::ITypedExpr> toLambdaExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

 private:
  /// Convert list literal to ArrayVector.
  // 作用：将 Substrait 的列表常量（List Literal）转换为 Velox 的 ArrayVector。
  ArrayVectorPtr literalsToArrayVector(const ::substrait::Expression::Literal& literal);
  /// Convert map literal to MapVector.
  // 作用：将 Substrait 的 Map 常量转换为 Velox 的 MapVector。
  MapVectorPtr literalsToMapVector(const ::substrait::Expression::Literal& literal);
  // 通过回调函数 elementAtFunc 遍历 Substrait 的元素，统一构建出 Velox 的基础 VectorPtr。
  VectorPtr literalsToVector(
      const ::substrait::Expression::Literal& childLiteral,
      vector_size_t childSize,
      std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAtFunc);
  // 作用：将 Substrait 的结构体常量（Struct Literal）转换为 Velox 的 RowVector。
  RowVectorPtr literalsToRowVector(const ::substrait::Expression::Literal& structLiteral);

  /// Memory pool.
  // 内存池指针。
  // 在转换常量（Literal）时，可能需要分配内存来创建 Velox 的 Vector（如数组或字符串常量），该指针用于管理这些内存。
  memory::MemoryPool* pool_;

  /// The map storing the relations between the function id and the function
  /// name.
  // 函数 ID 映射表。
  // Substrait 在序列化时使用整数 ID 引用函数，此属性记录了这些 ID 对应的实际函数名（如 "add", "regexp_replace"）。
  std::unordered_map<uint64_t, std::string> functionMap_;

  // The map storing the Substrait extract function input field and velox
  // function name.
  // 日期提取函数映射（静态）。
  // 专门用于将 Substrait 的 EXTRACT 逻辑映射到 Velox 对应的日期处理函数上（例如将 "YEAR" 映射到 Velox 的 year() 函数）。
  static std::unordered_map<std::string, std::string> extractDatetimeFunctionMap_;
};

} // namespace gluten
