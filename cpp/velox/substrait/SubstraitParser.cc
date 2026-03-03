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

#include "SubstraitParser.h"
#include "TypeUtils.h"
#include "velox/common/base/Exceptions.h"

#include "VeloxSubstraitSignature.h"

namespace gluten {

// 负责将 Substrait 协议定义的类型 映射为 Velox 引擎内部的数据类型 (TypePtr)。
// 返回值：TypePtr，这是 Velox 中指向类型对象的智能指针。
TypePtr SubstraitParser::parseType(const ::substrait::Type& substraitType, bool asLowerCase) {
  switch (substraitType.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
      return BOOLEAN();
    case ::substrait::Type::KindCase::kI8:
      return TINYINT();
    case ::substrait::Type::KindCase::kI16:
      return SMALLINT();
    case ::substrait::Type::KindCase::kI32:
      return INTEGER();
    case ::substrait::Type::KindCase::kI64:
      return BIGINT();
    case ::substrait::Type::KindCase::kFp32:
      return REAL();
    case ::substrait::Type::KindCase::kFp64:
      return DOUBLE();
    case ::substrait::Type::KindCase::kString:
      return VARCHAR();
    case ::substrait::Type::KindCase::kBinary:
      return VARBINARY();
    case ::substrait::Type::KindCase::kStruct: {
      const auto& substraitStruct = substraitType.struct_();
      const auto& structTypes = substraitStruct.types();
      const auto& structNames = substraitStruct.names();
      bool nameProvided = structTypes.size() == structNames.size();
      std::vector<TypePtr> types;
      std::vector<std::string> names;
      for (int i = 0; i < structTypes.size(); i++) {
        types.emplace_back(parseType(structTypes[i], asLowerCase));
        std::string fieldName = nameProvided ? structNames[i] : "col_" + std::to_string(i);
        if (asLowerCase) {
          folly::toLowerAscii(fieldName);
        }
        names.emplace_back(fieldName);
      }
      return ROW(std::move(names), std::move(types));
    }
    case ::substrait::Type::KindCase::kList: {
      const auto& fieldType = substraitType.list().type();
      return ARRAY(parseType(fieldType, asLowerCase));
    }
    case ::substrait::Type::KindCase::kMap: {
      const auto& sMap = substraitType.map();
      const auto& keyType = sMap.key();
      const auto& valueType = sMap.value();
      return MAP(parseType(keyType, asLowerCase), parseType(valueType, asLowerCase));
    }
    case ::substrait::Type::KindCase::kUserDefined:
      // We only support UNKNOWN type to handle the null literal whose type is
      // not known.
      return UNKNOWN();
    case ::substrait::Type::KindCase::kDate:
      return DATE();
    case ::substrait::Type::KindCase::kTimestampTz:
      return TIMESTAMP();
    case ::substrait::Type::KindCase::kDecimal: {
      auto precision = substraitType.decimal().precision();
      auto scale = substraitType.decimal().scale();
      return DECIMAL(precision, scale);
    }
    case ::substrait::Type::KindCase::kIntervalYear: {
      return INTERVAL_YEAR_MONTH();
    }
    case ::substrait::Type::KindCase::kNothing:
      return UNKNOWN();
    default:
      VELOX_NYI("Parsing for Substrait type not supported: {}", substraitType.DebugString());
  }
}
// 解析 Substrait 计划中 Schema（表结构定义） 的入口逻辑。
// 虽然它的名字叫 parseNamedStruct，但其内部逻辑反映了 Substrait 协议的一个有趣特点：结构定义（Types）与字段名称（Names）在协议层面是分离存储的。
// 输入：namedStruct 是 Substrait 的顶级结构对象，通常代表一个算子的输出 Schema；asLowerCase 决定是否将类型中的字符串处理为小写。
std::vector<TypePtr> SubstraitParser::parseNamedStruct(const ::substrait::NamedStruct& namedStruct, bool asLowerCase) {
  // Note that "names" are not used.

  // Parse Struct.
  const auto& substraitStruct = namedStruct.struct_();
  const auto& substraitTypes = substraitStruct.types();
  std::vector<TypePtr> typeList;
  // 使用 reserve 预先分配 vector 的内存空间
  typeList.reserve(substraitTypes.size());
  for (const auto& type : substraitTypes) {
    typeList.emplace_back(parseType(type, asLowerCase));
  }
  return typeList;
}

// 负责解析 Substrait NamedStruct 中的列属性（Column Types）
// 在分布式文件系统（如 HDFS/S3）中读取数据时，并非所有列都存储在文件的数据块中。有些列是分区列（来自路径），有些是元数据列（如行号）。
// 该方法的作用就是将 Substrait 定义的列分类映射为 Velox 引擎能够识别的 ColumnType 逻辑。
// 输入参数：namedStruct 是输入的 Schema 协议对象；columnTypes 是用于存储转换结果的输出向量。
void SubstraitParser::parseColumnTypes(
    const ::substrait::NamedStruct& namedStruct,
    std::vector<ColumnType>& columnTypes) {
  const auto& columnsTypes = namedStruct.column_types();
  // 逻辑：如果 Substrait 计划中没有显式指定列类型（size == 0），则认为所有的列都是普通数据列（kRegular）
  if (columnsTypes.size() == 0) {
    // Regard all columns as regular columns.
    columnTypes.resize(namedStruct.names().size(), ColumnType::kRegular);
    return;
  } else {
    // 如果指定了列类型，那么类型的数量必须与字段名称的数量严格一致。如果不一致，说明协议包损坏或逻辑错误，直接触发 VELOX_CHECK 报错。
    VELOX_CHECK_EQ(columnsTypes.size(), namedStruct.names().size(), "Wrong size for column types and column names.");
  }

  columnTypes.reserve(columnsTypes.size());
  for (const auto& columnType : columnsTypes) {
    // 开始遍历 Substrait 的枚举值，并将其“翻译”为 Velox 的枚举：
    switch (columnType) {
      case ::substrait::NamedStruct::NORMAL_COL:
        // 读取策略：如果是 kRegular，引擎会去读取 Parquet/ORC 文件的二进制流。
        columnTypes.push_back(ColumnType::kRegular);
        break;
      case ::substrait::NamedStruct::PARTITION_COL:
        // 数据填充：如果是 kPartitionKey，引擎会忽略文件内容，转而从算子上下文的 partitionValues 中取值。
        columnTypes.push_back(ColumnType::kPartitionKey);
        break;
      case ::substrait::NamedStruct::METADATA_COL:
        columnTypes.push_back(ColumnType::kSynthesized);
        break;
      case ::substrait::NamedStruct::ROWINDEX_COL:
        columnTypes.push_back(ColumnType::kRowIndex);
        break;
      default:
        VELOX_FAIL("Unspecified column type.");
    }
  }
  return;
}
// 核心作用是解析 Substrait 表达式中的字段引用索引（Column Index）。
// 在 Substrait 协议中，字段通常不是通过名字（如 "age"）引用的，而是通过它们在输入数据流中的位置索引（如 field: 0）来引用的。该方法就是负责从复杂的引用结构中提取出这个数字。
// 入参：refSegment 是 Substrait 协议定义的引用片段；fieldIndex 是一个引用参数，用于将解析出的索引值返回给调用者。
bool SubstraitParser::parseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment& refSegment,
    uint32_t& fieldIndex) {
  // 首先通过 reference_type_case() 判断这个引用属于哪种类型（Substrait 支持多种引用方式，如结构体字段、列表元素、Map 键等）
  auto typeCase = refSegment.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::ReferenceSegment::ReferenceTypeCase::kStructField: {
      // 原因：如果包含 child，意味着这不仅是一个列引用，还是一个深度嵌套引用（例如引用 StructA.fieldB.fieldC）
      // 当前限制：此处的逻辑明确表示目前不支持在这种简单的 parseReferenceSegment 逻辑中解析深层嵌套的子字段索引，遇到此类情况返回 false 触发上层降级或报错。
      if (refSegment.struct_field().has_child()) {
        // To parse subfield index is not supported.
        return false;
      }
      // 调用 .field() 获取协议中的整数索引值。
      fieldIndex = refSegment.struct_field().field();
      if (fieldIndex < 0) {
        return false;
      }
      return true;
    }
    default:
      VELOX_NYI("Substrait conversion not supported for ReferenceSegment '{}'", std::to_string(typeCase));
  }
}
// 非常实用的辅助工具函数
// 主要作用是在 Native 层构建计算计划树（Plan Tree）时，为算子的输出列生成一组具有统一前缀和序列编号的临时名称。
std::vector<std::string> SubstraitParser::makeNames(const std::string& prefix, int size) {
  std::vector<std::string> names;
  names.reserve(size);
  for (int i = 0; i < size; i++) {
    names.emplace_back(fmt::format("{}_{}", prefix, i));
  }
  return names;
}
// 在分布式查询执行中，一个复杂的查询会被拆解成许多算子节点（Nodes）。这个函数的作用是为执行计划树中某个特定算子节点产生的特定列生成一个唯一的、可追溯的名称。
std::string SubstraitParser::makeNodeName(int nodeId, int colIdx) {
  return fmt::format("n{}_{}", nodeId, colIdx);
}
// 是 makeNodeName 的逆向解析函数。
// 在 Gluten 的 Native 计划构建过程中，有些元数据会将信息存储在节点名称字符串中。该方法的作用是从像 "n101_5" 这样的合成列名中，精准地提取出最后的列索引数字（即本例中的 5）。
int SubstraitParser::getIdxFromNodeName(const std::string& nodeName) {
  // Get the position of "_" in the function name.
  std::size_t pos = nodeName.find("_");
  if (pos == std::string::npos) {
    VELOX_FAIL("Invalid node name.");
  }
  if (pos == nodeName.size() - 1) {
    VELOX_FAIL("Invalid node name.");
  }
  // Get the column index.
  std::string colIdx = nodeName.substr(pos + 1);
  try {
    return stoi(colIdx);
  } catch (const std::exception& err) {
    VELOX_FAIL(err.what());
  }
}
// Native 层解析 Substrait 计划时处理**函数调用（Functions）**的关键步骤。
// 入参 functionMap：这是一个映射表，Key 是函数的数字 ID（uint64_t），Value 是对应的函数完整签名字符串（std::string）。
// 入参 id：当前 Substrait 算子中引用的函数 ID。
std::string SubstraitParser::findFunctionSpec(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) {
  auto x = functionMap.find(id);
  if (x == functionMap.end()) {
    VELOX_FAIL("Could not find function id {} in function map.", id);
  }
  return x->second;
}

// TODO Refactor using Bison.
// 从复杂的函数签名中提取出基础函数名。
// 在 Substrait 协议中，函数通常以 函数名:参数类型 的格式表示（例如 add:i32_i32 或 lte:str_str）。
// 为了让 Native 引擎（如 Velox）能够识别该函数，首先需要去掉分隔符及其后面的类型后缀。
// 入参 signature：完整的 Substrait 函数签名字符串（如 "add:i32_i32"）。
// 入参 delimiter：分隔符，默认为 ":"。
std::string SubstraitParser::getNameBeforeDelimiter(const std::string& signature, const std::string& delimiter) {
  std::size_t pos = signature.find(delimiter);
  if (pos == std::string::npos) {
  // 如果没有找到分隔符（npos），说明这个字符串就是一个纯函数名，或者不符合复合命名的规范。
    return signature;
  }
  return signature.substr(0, pos);
}
// 作用是从 Substrait 函数签名中提取出参数的类型列表。
// 在 Substrait 规范中，复合函数名的格式通常为 函数名:参数1类型_参数2类型_...（例如 add:i32_i64）。这个方法负责解析冒号 : 之后的部分，并按顺序拆分出各个参数类型的缩写。
std::vector<std::string> SubstraitParser::getSubFunctionTypes(const std::string& substraitFunction) {
  // Get the position of ":" in the function name.
  size_t pos = substraitFunction.find(":");
  // Get the parameter types.
  std::vector<std::string> types;
  if (pos == std::string::npos || pos == substraitFunction.size() - 1) {
    return types;
  }
  // Extract input types with delimiter.
  for (;;) {
    const size_t endPos = substraitFunction.find("_", pos + 1);
    if (endPos == std::string::npos) {
      std::string typeName = substraitFunction.substr(pos + 1);
      if (typeName != "opt" && typeName != "req") {
        types.emplace_back(typeName);
      }
      break;
    }

    const std::string typeName = substraitFunction.substr(pos + 1, endPos - pos - 1);
    if (typeName != "opt" && typeName != "req") {
      types.emplace_back(typeName);
    }
    pos = endPos;
  }
  return types;
}
// 函数解析流程的集大成者，将之前解析出的各种碎片信息（ID、签名、名称、类型）整合在一起，最终确定 Velox 引擎中对应的函数名。
std::string SubstraitParser::findVeloxFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) {
  // 将数字 ID 还原为完整的 Substrait 签名字符串（如 "add:dec_dec" 或 "lte:i32_i32"）。
  std::string funcSpec = findFunctionSpec(functionMap, id);
  // 剥离类型后缀，只拿到核心动作名称（如 "add"）
  std::string funcName = getNameBeforeDelimiter(funcSpec);
  // 获取函数参数的所有类型缩写（如 {"dec", "dec"}）。
  std::vector<std::string> types = getSubFunctionTypes(funcSpec);
  bool isDecimal = false;
  // 高精度数值 (Decimal) 探测
  // 遍历类型列表，查找是否包含字符串 "dec"。
  // 原因：在 Velox 中，处理普通数值（如 Integer/Double）的函数名和处理 Decimal 的函数名可能不同。
  // 例如，普通的 add 映射为 plus，但 Decimal 的 add 在某些实现中可能需要特殊的算子映射。这里通过一个布尔标记 isDecimal 来捕捉这个特征
  for (const auto& type : types) {
    if (type.find("dec") != std::string::npos) {
      isDecimal = true;
      break;
    }
  }
  // 根据基础名和是否为 Decimal，查表返回 Velox 真正注册的函数名。
  return mapToVeloxFunction(funcName, isDecimal);
}
// 函数解析流程的最后一站。
// 其核心任务是将 Substrait 的通用函数名（如 lt, equal）最终转换为 Velox 引擎中注册的真实函数名。特别地，它还处理了 Decimal（高精度小数） 类型的特殊命名规则。
std::string SubstraitParser::mapToVeloxFunction(const std::string& substraitFunction, bool isDecimal) {
  // 首先在预定义的静态映射表 substraitVeloxFunctionMap_ 中查找传入的 Substrait 函数名。
  auto it = substraitVeloxFunctionMap_.find(substraitFunction);
  if (isDecimal) {
   // 比较操作符：对于 lt (小于)、lte (小于等于) 等比较运算，Velox 要求使用特定的 Decimal 版本。代码会将映射后的名字加上 decimal_ 前缀（例如 equal 映射后变为 eq，最终返回 decimal_eq）。
    if (substraitFunction == "lt" || substraitFunction == "lte" || substraitFunction == "gt" ||
        substraitFunction == "gte" || substraitFunction == "equal") {
      return "decimal_" + it->second;
    }
    if (substraitFunction == "round") {
      return "decimal_round";
    }
  }
  // 如果在映射表中找不到对应的条目，代码假设 Substrait 的函数名与 Velox 的函数名一致，直接原样返回。
  if (it != substraitVeloxFunctionMap_.end()) {
    return it->second;
  }
  // If not finding the mapping from Substrait function name to Velox function
  // name, the original Substrait function name will be used.
  return substraitFunction;
}
// 作用是从 Substrait 协议的扩展字段中提取特定的优化配置开关。
// 在 Substrait 协议中，AdvancedExtension 是一个预留的“口袋”，允许像 Gluten 这样的项目传递一些不属于 Substrait 标准、但对后端引擎优化至关重要的自定义配置。
// 入参 extension：Substrait 的高级扩展对象。
// 入参 config：要查找的配置键名（例如 "is_streaming"）。
bool SubstraitParser::configSetInOptimization(
    const ::substrait::extensions::AdvancedExtension& extension,
    const std::string& config) {
  if (extension.has_optimization()) {
    google::protobuf::StringValue msg;
    // 调用 UnpackTo 将二进制数据还原为字符串对象 msg。
    extension.optimization().UnpackTo(&msg);
    std::size_t pos = msg.value().find(config);
    // 获取紧跟在键名之后的第一个字符，判断它是否为 "1"。
    if ((pos != std::string::npos) && (msg.value().substr(pos + config.size(), 1) == "1")) {
      return true;
    }
  }
  return false;
}
// 作用是从 Substrait 的高级扩展（AdvancedExtension）中验证当前正在处理的窗口函数是否与指定的优化目标匹配。
// 入参 extension：Substrait 计划中的扩展信息。
// 入参 targetFunction：期望匹配的目标函数名（例如 "row_number" 或 "rank"）。
// 变量 config：硬编码的键名字符串，表示在扩展信息中寻找以 window_function= 开头的配置。
bool SubstraitParser::checkWindowFunction(
    const ::substrait::extensions::AdvancedExtension& extension,
    const std::string& targetFunction) {
  const std::string config = "window_function=";
  if (extension.has_optimization()) {
    google::protobuf::StringValue msg;
    extension.optimization().UnpackTo(&msg);
    std::size_t pos = msg.value().find(config);
    // 截取与 targetFunction 长度相等的字符串，并判断它们是否完全一致。
    if ((pos != std::string::npos) && (msg.value().size() >= targetFunction.size()) &&
        (msg.value().substr(pos + config.size(), targetFunction.size()) == targetFunction)) {
      return true;
    }
  }
  return false;
}
// 作用是将一个 Substrait 函数签名字符串（Signature）直接转换为一组 Velox 的类型对象（TypePtr）
std::vector<TypePtr> SubstraitParser::sigToTypes(const std::string& signature) {
  std::vector<std::string> typeStrs = SubstraitParser::getSubFunctionTypes(signature);
  std::vector<TypePtr> types;
  types.reserve(typeStrs.size());
  for (const auto& typeStr : typeStrs) {
    types.emplace_back(VeloxSubstraitSignature::fromSubstraitSignature(typeStr));
  }
  return types;
}
// 从 Substrait 的 Literal（字面量，即 SQL 中的常量，如 10, 'apple', 3.14）对象中提取出对应的 C++ 原始数值。
template <typename T>
T SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& /* literal */) {
  VELOX_NYI();
}

// 在 C++ 中，当基础模板无法满足特定类型的处理逻辑时，我们会为该类型编写一个特化版本。这里特化的目标类型是 std::shared_ptr<void>。
template <>
std::shared_ptr<void> gluten::SubstraitParser::getLiteralValue(const substrait::Expression_Literal& literal) {
  return nullptr;
}

template <>
facebook::velox::UnknownValue gluten::SubstraitParser::getLiteralValue(const substrait::Expression_Literal& literal) {
  return UnknownValue();
}

template <>
int8_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int8_t>(literal.i8());
}

template <>
int16_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int16_t>(literal.i16());
}

template <>
int32_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_date()) {
    return static_cast<int32_t>(literal.date());
  }
  return literal.i32();
}

template <>
int64_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_decimal()) {
    auto decimal = literal.decimal().value();
    int128_t decimalValue;
    memcpy(&decimalValue, decimal.c_str(), 16);
    return static_cast<int64_t>(decimalValue);
  }
  return literal.i64();
}

template <>
int128_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  auto decimal = literal.decimal().value();
  int128_t decimalValue;
  memcpy(&decimalValue, decimal.c_str(), 16);
  return HugeInt::build(static_cast<uint64_t>(decimalValue >> 64), static_cast<uint64_t>(decimalValue));
}

template <>
double SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp64();
}

template <>
float SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp32();
}

template <>
bool SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.boolean();
}

template <>
Timestamp SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return Timestamp::fromMicros(literal.timestamp_tz());
}

template <>
StringView SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_string()) {
    return StringView(literal.string());
  } else if (literal.has_var_char()) {
    return StringView(literal.var_char().value());
  } else if (literal.has_binary()) {
    return StringView(literal.binary());
  } else {
    VELOX_FAIL("Unexpected string or binary literal");
  }
}

std::unordered_map<std::string, std::string> SubstraitParser::substraitVeloxFunctionMap_ = {
    {"is_not_null", "isnotnull"}, /*Spark functions.*/
    {"is_null", "isnull"},
    {"equal", "equalto"},
    {"equal_null_safe", "equalnullsafe"},
    {"lt", "lessthan"},
    {"lte", "lessthanorequal"},
    {"gt", "greaterthan"},
    {"gte", "greaterthanorequal"},
    {"char_length", "length"},
    {"strpos", "instr"},
    {"ends_with", "endswith"},
    {"starts_with", "startswith"},
    {"named_struct", "row_constructor"},
    {"bit_or", "bitwise_or_agg"},
    {"bit_and", "bitwise_and_agg"},
    {"murmur3hash", "hash_with_seed"},
    {"xxhash64", "xxhash64_with_seed"},
    {"modulus", "remainder"},
    {"negative", "unaryminus"},
    {"get_array_item", "get"}};

const std::unordered_map<std::string, std::string> SubstraitParser::typeMap_ = {
    {"bool", "BOOLEAN"},
    {"i8", "TINYINT"},
    {"i16", "SMALLINT"},
    {"i32", "INTEGER"},
    {"i64", "BIGINT"},
    {"fp32", "REAL"},
    {"fp64", "DOUBLE"},
    {"date", "DATE"},
    {"ts", "TIMESTAMP"},
    {"str", "VARCHAR"},
    {"vbin", "VARBINARY"},
    {"decShort", "SHORT_DECIMAL"},
    {"decLong", "HUGEINT"}};

} // namespace gluten
