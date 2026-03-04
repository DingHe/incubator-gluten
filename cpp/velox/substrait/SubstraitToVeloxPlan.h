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

#include "SubstraitToVeloxExpr.h"
#include "TypeUtils.h"
#include "velox/connectors/hive/FileProperties.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"

namespace gluten {
class ResultIterator;
// 在 Apache Gluten 项目中，SplitInfo 是一个至关重要的结构化元数据类。它充当了逻辑执行计划与物理数据源之间的桥梁。
// SplitInfo 的主要作用是描述如何读取数据源。在分布式计算中，一个大的查询会被拆分成多个分区（Partitions），每个分区由一个 Task 处理。SplitInfo 记录了一个 Task 在执行时所需的全部物理信息，包括：
// 去哪里读（文件路径、偏移量、长度）。
// 以什么格式读（Parquet, ORC 等）。
// 读取时的环境信息（分区列、Schema 等）。
// 它是从 Substrait 计划中的 ReadRel（读取关系）转换而来的，最终会被 Velox 引擎用来构建具体的 ConnectorSplit。
struct SplitInfo {
  // 定义叶子节点的枚举类型。
  enum class LeafType {
    /// A streaming node that accepts iterator splits.
    // 流式节点，通常用于处理上一个 Stage 传来的迭代器数据。
    SPLIT_AWARE_STREAM = 0,
    /// A table scan node that accepts scan splits.
    // 传统的表扫描节点，负责从文件系统中读取数据。
    TABLE_SCAN = 1,
    /// A leaf node that doesn't rely on splits.
    // 不依赖分片的普通叶子节点（默认值）
    TRIVIAL_LEAF = 2
  };

  /// The type of the associated Velox leaf query plan node.
  // 标识与此 SplitInfo 关联的 Velox 计划节点的类型。这决定了后端如何处理该分片。
  LeafType leafType = LeafType::TRIVIAL_LEAF;

  /// The Partition index.
  // 记录当前分片所属的分区索引。在 Spark 任务中，这通常对应于 Task 的 Partition ID。
  u_int32_t partitionIndex;

  /// The partition columns associated with partitioned table.
  // 存储分区列的信息。例如在路径 year=2024/month=03 中，这里会记录键值对。
  std::vector<std::unordered_map<std::string, std::string>> partitionColumns;

  /// The metadata columns associated with partitioned table.
  // 存储与表关联的元数据列（如隐藏列、内部文件元数据等）。
  std::vector<std::unordered_map<std::string, std::string>> metadataColumns;

  /// The file paths to be scanned.
  // 存储待扫描文件的完整路径列表。
  std::vector<std::string> paths;

  /// The file starts in the scan.
  // 记录每个文件扫描的起始字节偏移量。这对于处理大型文件（按块/Row Group 切分）至关重要。
  std::vector<u_int64_t> starts;

  /// The lengths to be scanned.
  // 记录每个文件需要扫描的字节长度。
  std::vector<u_int64_t> lengths;

  /// The file format of the files to be scanned.
  // 指定文件的存储格式（例如 PARQUET, ORC 等），以便 Velox 调用正确的 Reader。
  dwio::common::FileFormat format;

  /// The file sizes and modification times of the files to be scanned.
  // 存储文件的额外属性，如文件大小、最后修改时间等。这常用于文件一致性检查。
  std::vector<std::optional<facebook::velox::FileProperties>> properties;

  /// The schema of the table being scanned.
  // 记录正在被扫描的表的原始 Schema（结构）。这是 Velox 构建数据流时的类型基准。
  RowTypePtr tableSchema;

  /// Make SplitInfo polymorphic
  virtual ~SplitInfo() = default;
  // 检查是否可以使用 cuDF 连接器。
  // 这是一个特定于硬件加速（GPU）的判断函数。它会根据当前 SplitInfo 中的文件格式、Schema 等信息，判断该分片是否能够交给 NVIDIA 的 cuDF 库（Gluten 的另一个后端选项）进行 GPU 加速处理。
  bool canUseCudfConnector();
};

/// This class is used to convert the Substrait plan into Velox plan.
// 在 Apache Gluten 项目中，SubstraitToVeloxPlanConverter 是一个核心转换器类。它是联系 Substrait 协议与 Velox 引擎的“神经网络”，负责将抽象的算子逻辑转化为具体的可执行节点。
// 核心作用是深度解析（Deep Parsing）。
// 它不仅调用 SubstraitParser 来处理基础类型，还负责算子层级的重构。它递归地遍历 Substrait 的关系树（Relation Tree），根据每一个 Rel 节点（如 Join, Aggregate, Filter）构造出 Velox 对应的 PlanNode。
class SubstraitToVeloxPlanConverter {
 public:
  explicit SubstraitToVeloxPlanConverter(
      memory::MemoryPool* pool,
      const facebook::velox::config::ConfigBase* veloxCfg,
      const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      const std::optional<std::string> writeFileName = std::nullopt,
      bool validationMode = false)
      : pool_(pool),
        veloxCfg_(veloxCfg),
        inputIters_(inputIters),
        writeFilesTempPath_(writeFilesTempPath),
        writeFileName_(writeFileName),
        validationMode_(validationMode) {
    VELOX_USER_CHECK_NOT_NULL(veloxCfg_);
  }

  /// Used to convert Substrait WriteRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WriteRel& writeRel);

  /// Used to convert Substrait ExpandRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ExpandRel& expandRel);

  /// Used to convert Substrait GenerateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::GenerateRel& generateRel);

  /// Used to convert Substrait WindowRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowRel& windowRel);

  /// Used to convert Substrait WindowGroupLimitRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowGroupLimitRel& windowGroupLimitRel);

  /// Used to convert Substrait SetRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SetRel& setRel);

  /// Used to convert Substrait JoinRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::JoinRel& joinRel);

  /// Used to convert Substrait CrossRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::CrossRel& crossRel);

  /// Used to convert Substrait AggregateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::AggregateRel& aggRel);

  /// Convert Substrait ProjectRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ProjectRel& projectRel);

  /// Convert Substrait FilterRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FilterRel& filterRel);

  /// Convert Substrait FetchRel into Velox LimitNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FetchRel& fetchRel);

  /// Convert Substrait TopNRel into Velox TopNNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::TopNRel& topNRel);

  /// Convert Substrait ReadRel into Velox Values Node.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& readRel, const RowTypePtr& type);

  /// Convert Substrait SortRel into Velox OrderByNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SortRel& sortRel);

  /// Convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  /// FileProperties: the file sizes and modification times of the files to be scanned.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& sRead);

  // Construct a table scan node accepting value streams as input.
  // 构建流式读取节点。用于处理不是文件、而是内存迭代器的数据源。
  core::PlanNodePtr constructValueStreamNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  // Construct a cuDF value stream node.
  core::PlanNodePtr constructCudfValueStreamNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  // This is only used in benchmark and enable query trace, which will load all the data to ValuesNode.
  core::PlanNodePtr constructValuesNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  /// Used to convert Substrait Rel into Velox PlanNode.
  // 路由函数。
  // 根据 Rel 的类型（Project, Filter 等）分发给具体的转换方法。
  core::PlanNodePtr toVeloxPlan(const ::substrait::Rel& sRel);

  /// Used to convert Substrait RelRoot into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::RelRoot& sRoot);

  /// Used to convert Substrait Plan into Velox PlanNode.
  // 总入口。
  // 解析整个 Substrait 计划文件，初始化函数映射，并开始递归转换。
  core::PlanNodePtr toVeloxPlan(const ::substrait::Plan& substraitPlan);

  // return the raw ptr of ExprConverter
  SubstraitVeloxExprConverter* getExprConverter() {
    return exprConverter_.get();
  }

  /// Used to construct the function map between the index
  /// and the Substrait function name. Initialize the expression
  /// converter based on the constructed function map.
  // 在处理计划前，先扫描计划头部的扩展信息，建立 ID 到函数名的映射，这是后续转换表达式的基础。
  void constructFunctionMap(const ::substrait::Plan& substraitPlan);

  void constructFunctionMap(std::unordered_map<uint64_t, std::string> substraitPlan);

  /// Will return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the splitInfo map used by this plan converter.
  const std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() const {
    return splitInfoMap_;
  }

  /// Used to insert certain plan node as input. The plan node
  /// id will start from the setted one.
  void insertInputNode(uint64_t inputIdx, const std::shared_ptr<const core::PlanNode>& inputNode, int planNodeId) {
    inputNodesMap_[inputIdx] = inputNode;
    planNodeId_ = planNodeId;
  }

  void setSplitInfos(std::vector<std::shared_ptr<SplitInfo>> splitInfos) {
    splitInfos_ = splitInfos;
  }

  /// The input iterators not inlined to VeloxPlan. They should be then manually added to the Velox task
  /// via WholeStageResultIterator#addIteratorSplits. Empty if no input iterators remaining.
  const std::vector<std::shared_ptr<ResultIterator>>& remainingInputIterators() const {
    return inputIters_;
  }

  /// Used to check if ReadRel specifies an input of stream.
  /// If yes, the index of input stream will be returned.
  /// If not, -1 will be returned.
  // 检查 ReadRel 是否指向一个特定的输入流，并返回其索引。
  int32_t getStreamIndex(const ::substrait::ReadRel& sRel);

  /// Used to find the function specification in the constructed function map.
  std::string findFuncSpec(uint64_t id);

  /// Extract join keys from joinExpression.
  /// joinExpression is a boolean condition that describes whether each record
  /// from the left set “match” the record from the right set. The condition
  /// must only include the following operations: AND, ==, field references.
  /// Field references correspond to the direct output order of the data.
  // 算法逻辑。从复杂的 Join 布尔表达式中分离出左表和右表的连接键（Join Keys）。
  void extractJoinKeys(
      const ::substrait::Expression& joinExpression,
      std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
      std::vector<const ::substrait::Expression::FieldReference*>& rightExprs);

  /// Get aggregation step from AggregateRel.
  /// If returned Partial, it means the aggregate generated can leveraging flushing and abandoning like
  /// what streaming pre-aggregation can do in MPP databases.
  core::AggregationNode::Step toAggregationStep(const ::substrait::AggregateRel& sAgg);

  /// Get aggregation function step for AggregateFunction.
  /// The returned step value will be used to decide which Velox aggregate function or companion function
  /// is used for the actual data processing.
  core::AggregationNode::Step toAggregationFunctionStep(const ::substrait::AggregateFunction& sAggFuc);

  /// We use companion functions if the aggregate is not single.
  std::string toAggregationFunctionName(
      const std::string& baseName,
      const core::AggregationNode::Step& step,
      const TypePtr& resultType);

  /// Helper Function to convert Substrait sortField to Velox sortingKeys and
  /// sortingOrders.
  /// Note that, this method would deduplicate the sorting keys which have the same field name.
  // 排序字段预处理。提取排序键并识别升降序及 Null 排序规则。
  std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>> processSortField(
      const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortField,
      const RowTypePtr& inputType);

 private:
  /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
  /// is passed and check if emit is defined for this relation. Basically a
  /// ProjectNode is added on top of 'noEmitNode' to represent output order
  /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
  /// if output order is 'kDriect'.
  // 处理 Substrait 的 emit 特性（重排输出列）。
  // 如果 Rel 定义了输出顺序，它会在该节点上方自动插入一个 ProjectNode 进行对齐。
  core::PlanNodePtr processEmit(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& noEmitNode);

  /// Check the Substrait type extension only has one unknown extension.
  static bool checkTypeExtension(const ::substrait::Plan& substraitPlan);

  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  // 内部调用，生成下一个递增的节点 ID 字符串。
  std::string nextPlanNodeId();

  /// Used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// Helper function to convert the input of Substrait Rel to Velox Node.
  template <typename T>
  core::PlanNodePtr convertSingleInput(T rel) {
    VELOX_CHECK(rel.has_input(), "Child Rel is expected here.");
    return toVeloxPlan(rel.input());
  }
  // 窗口定义转换。
  // 将 Substrait 的窗口范围（如 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW）转换为 Velox 内部的 Frame 结构。
  const core::WindowNode::Frame createWindowFrame(
      const ::substrait::Expression_WindowFunction_Bound& lower_bound,
      const ::substrait::Expression_WindowFunction_Bound& upper_bound,
      const ::substrait::WindowType& type,
      const RowTypePtr& inputType);

  /// The unique identification for each PlanNode.
  // 计划节点 ID 计数器。
  // 用于为生成的每个 Velox 节点分配一个唯一的字符串 ID（如 "0", "1"）。
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  // 函数映射表。
  // 存储 Substrait 计划中定义的函数 ID 与函数全名（Signature）的映射。
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the split stats for each PlanNode.
  // 节点与分片信息的关联表。
  // 记录哪些 Velox 节点（通常是扫表节点）需要读取哪些物理分片。
  std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>> splitInfoMap_;

  /// The map storing the pre-built plan nodes which can be accessed through
  /// index. This map is only used when the computation of a Substrait plan
  /// depends on other input nodes.
  // 预构建输入节点表。
  // 用于处理非树状结构或需要引用外部节点的特殊计划。
  std::unordered_map<uint64_t, std::shared_ptr<const core::PlanNode>> inputNodesMap_;

  int32_t splitInfoIdx_{0};
  // 存储所有的 SplitInfo 对象列表。
  std::vector<std::shared_ptr<SplitInfo>> splitInfos_;

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  // 表达式转换器。
  // 这是一个极其重要的组件，专门负责将 Substrait 的表达式（如 a + b > 10）转换为 Velox 的表达式树。
  std::unique_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// Memory pool.
  // 内存池。
  // 用于转换过程中临时元数据或表达式对象的内存分配。
  memory::MemoryPool* pool_;

  /// A map of custom configs.
  // 全局配置。
  // 传递来自 Spark 的环境参数。
  const facebook::velox::config::ConfigBase* veloxCfg_;

  /// Input row-vectors for query trace mode (ValuesNode / cuDF ValueStream support)
  // 输入迭代器。
  // 在处理来自上游 Stage 的结果流时使用。
  std::vector<std::shared_ptr<ResultIterator>> inputIters_;

  /// The temporary path used to write files.
  // 写文件相关的路径信息，用于 WriteRel 转换。
  std::optional<std::string> writeFilesTempPath_;
  std::optional<std::string> writeFileName_;

  /// A flag used to specify validation.
  // 校验模式开关。
  bool validationMode_ = false;
};

} // namespace gluten
