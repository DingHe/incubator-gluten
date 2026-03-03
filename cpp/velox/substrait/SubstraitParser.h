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

#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"

#include <google/protobuf/wrappers.pb.h>

#include "velox/connectors/hive/TableHandle.h"
#include "velox/type/Type.h"

namespace gluten {

typedef ::facebook::velox::connector::hive::HiveColumnHandle::ColumnType ColumnType;

/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
// SubstraitParser 的作用就是在 Native 端（计算引擎侧）将 Substrait 协议解码并映射为 Native 引擎（如 Velox）能理解的数据结构。
// 其核心职责包括：
// 协议解析：解析 Substrait 的 Protobuf 对象（如 Type, NamedStruct, Literal）。
// 类型映射：将 Substrait 定义的标准类型转换为 Velox 的 TypePtr。
// 函数适配：处理 Substrait 函数签名（Signature）与 Velox 函数名之间的转换。
// 元数据辅助：处理字段索引、节点命名等计算计划树构建过程中的辅助逻辑。
class SubstraitParser {
 public:
  /// Used to parse Substrait NamedStruct.
  // 解析 Substrait 的 NamedStruct 并返回 Velox 类型列表。
  // 参数：namedStruct (Protobuf 对象), asLowerCase (是否转小写)。
  // 详细说明：NamedStruct 通常代表表结构或 Schema。该方法提取列的类型信息并按顺序转换为 Velox 的类型指针。
  static std::vector<facebook::velox::TypePtr> parseNamedStruct(
      const ::substrait::NamedStruct& namedStruct,
      bool asLowerCase = false);

  /// Used to parse column types from Substrait NamedStruct.
  // 专门解析 NamedStruct 中的列类型。
  // 参数：columnTypes (输出参数，存入 HiveColumnHandle 类型的列信息)。
  static void parseColumnTypes(const ::substrait::NamedStruct& namedStruct, std::vector<ColumnType>& columnTypes);

  /// Parse Substrait Type to Velox type.
  // 将单个 Substrait 类型转换为 Velox 类型。
  // 处理基础类型（I32, String等）和嵌套类型（List, Map, Struct）。
  static facebook::velox::TypePtr parseType(const ::substrait::Type& substraitType, bool asLowerCase = false);

  /// Parse Substrait ReferenceSegment and extract the field index. Return false if the segment is not a valid unnested
  /// field.
  // 解析 Substrait 的字段引用片段。
  // 参数：refSegment, fieldIndex (输出字段索引)。
  // 从 Substrait 的 ReferenceSegment（通常代表对某一列的引用）中提取列的偏移量索引。
  static bool parseReferenceSegment(const ::substrait::Expression::ReferenceSegment& refSegment, uint32_t& fieldIndex);

  /// Make names in the format of {prefix}_{index}.
  // 用于生成和解析中间节点的临时名称。
  // 在构建计算树时，为了区分不同的投影列，会生成类似 n101_0 格式的唯一名称，这些方法保证了名称的标准化。
  static std::vector<std::string> makeNames(const std::string& prefix, int size);

  /// Make node name in the format of n{nodeId}_{colIdx}.
  // 用于生成和解析中间节点的临时名称。
  static std::string makeNodeName(int nodeId, int colIdx);

  /// Get the column index from a node name in the format of
  /// n{nodeId}_{colIdx}.
  // 用于生成和解析中间节点的临时名称。
  static int getIdxFromNodeName(const std::string& nodeName);

  /// Find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  // 根据函数 ID 在函数映射表中查找完整的函数规范（Specification）
  // Substrait 计划中函数调用通常只存 ID，需要通过此方法找回原本的签名（如 add:i32_i32）。
  static std::string findFunctionSpec(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id);

  /// Extracts the name of a function by splitting signature with delimiter.
  // 提取函数签名中的函数基础名。
  // 输入 add:i32_i32，分隔符 :，输出 add。
  static std::string getNameBeforeDelimiter(const std::string& signature, const std::string& delimiter = ":");

  /// This function is used get the types from the compound name.
  // 解析复合函数名中的参数类型部分。
  static std::vector<std::string> getSubFunctionTypes(const std::string& subFuncSpec);

  /// Used to find the Velox function name according to the function id
  /// from a pre-constructed function map.
  // 根据函数 ID 直接查找对应的 Velox 函数名。
  static std::string findVeloxFunction(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id);

  /// Map the Substrait function keyword into Velox function keyword.
  // 执行具体的 Substrait 关键字到 Velox 关键字的映射转化，支持 Decimal 特殊处理。
  static std::string mapToVeloxFunction(const std::string& substraitFunction, bool isDecimal);

  /// @brief Return whether a config is set as true in AdvancedExtension
  /// optimization.
  /// @param extension Substrait advanced extension.
  /// @param config the key string of a config.
  /// @return Whether the config is set as true.
  // 检查 Substrait 的 AdvancedExtension 中是否设置了某个配置项。
  // 用于读取从 Spark 端传过来的特殊优化策略或后端开关。
  static bool configSetInOptimization(const ::substrait::extensions::AdvancedExtension&, const std::string& config);

  /// @brief Return whether a config is set as true in AdvancedExtension
  /// optimization.
  /// @param extension Substrait advanced extension.
  /// @param target function
  /// @return Whether the target function is match.
  // 在扩展信息中校验窗口函数是否匹配目标函数。
  static bool checkWindowFunction(const ::substrait::extensions::AdvancedExtension&, const std::string& targetFunction);

  /// Extract input types from Substrait function signature.
  // 将函数签名（字符串）解析为 Velox 类型序列。
  // 将 i32_i64 转换为对应的 INTEGER 和 BIGINT 类型对象。
  static std::vector<facebook::velox::TypePtr> sigToTypes(const std::string& functionSig);

  // Get values for the different supported types.
  // 从 Substrait 的常量（Literal）中提取具体的数值。
  // 支持多种类型（如 int, long, string 等），将 Protobuf 的泛型数据转为 C++ 原生类型。
  template <typename T>
  static T getLiteralValue(const ::substrait::Expression::Literal& /* literal */);

 private:
  /// A map used for mapping Substrait function keywords into Velox functions'
  /// keywords. Key: the Substrait function keyword, Value: the Velox function
  /// keyword. For those functions with different names in Substrait and Velox,
  /// a mapping relation should be added here.
  // Substrait 到 Velox 的函数名映射表。
  // Substrait 定义了一套标准函数名（如 add），但 Velox 内部可能有不同的名称（如 plus）。此表用于处理这种命名差异。
  static std::unordered_map<std::string, std::string> substraitVeloxFunctionMap_;

  // The map is uesd for mapping substrait type.
  // Key: type in function name.
  // Value: substrait type name.
  // 函数签名中的类型字符串到 Substrait 类型名的映射。
  // 用于解析形如 add:i32_i64 中的类型部分。
  static const std::unordered_map<std::string, std::string> typeMap_;
};

} // namespace gluten
