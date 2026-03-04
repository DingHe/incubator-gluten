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

#include <velox/common/memory/MemoryPool.h>
#include <velox/core/PlanNode.h>
#include <velox/exec/Split.h>

#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"

namespace gluten {

// This class is used to convert the Substrait plan into Velox plan.
// VeloxPlanConverter 是一个核心的适配器类。如果说 SubstraitParser 是“翻译词典”，那么 VeloxPlanConverter 就是“总工程师”。
// VeloxPlanConverter 的主要职责是将 Substrait 格式的逻辑执行计划完整地转换成 Velox 引擎可运行的物理计划树 (PlanNode)。
// 在 Gluten 的架构中，Spark 将 SQL 转换成 Substrait 协议发送给 Native 层。
// 这个类作为转换流程的总入口，它协调内存管理、配置传递，并调用底层的转换组件，最终生成一个能够被 Velox 执行器（Task）直接加载运行的计划。
class VeloxPlanConverter {
 public:
  // 初始化转换上下文。
  explicit VeloxPlanConverter(
      // veloxPool：为转换过程中可能产生的元数据分配内存。
      facebook::velox::memory::MemoryPool* veloxPool,
      // 传递全局配置。
      const facebook::velox::config::ConfigBase* veloxCfg,
      // 传入已经存在的输入迭代器（通常是上一个 Stage 的计算结果）
      const std::vector<std::shared_ptr<ResultIterator>>& rowVectors,
      // 针对写文件操作（如 INSERT OVERWRITE）的路径信息。
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      const std::optional<std::string> writeFileName = std::nullopt,
      // 设置是否仅开启校验模式。
      bool validationMode = false);

  // 执行 “协议 -> 计划树” 的翻译动作。
  // 返回一个 PlanNode 指针，这是 Velox 计划树的根节点，拿到它就可以创建 Task 开始计算数据了。
  std::shared_ptr<const facebook::velox::core::PlanNode> toVeloxPlan(
      // 接收一个完整的 substrait::Plan 对象。
      const ::substrait::Plan& substraitPlan,
      // 接收 localFiles（本地文件分片信息），这对应了 Spark 扫描的物理文件位置。
      std::vector<::substrait::ReadRel_LocalFiles> localFiles);

  // 获取 数据分片映射表。
  const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() {
    return substraitVeloxPlanConverter_.splitInfos();
  }

  /// The input iterators not inlined to VeloxPlan. They should be then manually added to the Velox task
  /// via WholeStageResultIterator#addIteratorSplits. Empty if no input iterators remaining.
  // 获取 未被内联的输入迭代器。
  // 在某些复杂的流水线中，并非所有的输入数据都能直接嵌入到计划树内。如果某些数据源迭代器在转换后依然“游离”在外，必须通过这个方法取回，并在执行阶段手动添加到 Velox Task 中。
  const std::vector<std::shared_ptr<ResultIterator>>& remainingInputIterators() const {
    return substraitVeloxPlanConverter_.remainingInputIterators();
  }

 private:
  // 验证模式开关。
  // 当为 true 时，转换器会以更严格的规则检查 Substrait 计划（例如检查算子是否受支持、表达式是否合法），而不会真正准备执行。这通常用于 Spark 侧判断一个算子是否应该下推到 Native。
  bool validationMode_;
  // Velox 配置句柄。
  // 保存了来自 Spark 端的配置信息（如内存限制、特定的算子行为开关等），在构建计划节点时，有些算子需要这些配置来调整执行行为。
  const facebook::velox::config::ConfigBase* veloxCfg_;
  // 底层的核心转换执行器。
  // VeloxPlanConverter 将具体的转换请求委托给它。它负责维护转换过程中的中间状态，如 Split 信息和输入迭代器。
  SubstraitToVeloxPlanConverter substraitVeloxPlanConverter_;
};

} // namespace gluten
