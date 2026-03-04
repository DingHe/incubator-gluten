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

#include <glog/logging.h>

#include "compute/ProtobufUtils.h"
#include "compute/ResultIterator.h"
#include "memory/ColumnarBatch.h"
#include "memory/MemoryManager.h"
#include "memory/SplitAwareColumnarBatchIterator.h"
#include "operators/c2r/ColumnarToRow.h"
#include "operators/r2c/RowToColumnar.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "substrait/plan.pb.h"
#include "utils/ObjectStore.h"
#include "utils/WholeStageDumper.h"

namespace gluten {

class ResultIterator;

struct SparkTaskInfo {
  int32_t stageId{0}; // 属于哪个查询阶段。
  int32_t partitionId{0}; // 处理哪个分区。
  // Same as TID.
  int64_t taskId{0}; // Spark 级别的全局任务 ID。
  // virtual id for each backend internal use
  int32_t vId{0}; // 通常用于后端内部区分并行的 Pipeline 或线程。

  std::string toString() const {
    return "[Stage: " + std::to_string(stageId) + " TID: " + std::to_string(taskId) + " VID: " + std::to_string(vId) +
        "]";
  }

  friend std::ostream& operator<<(std::ostream& os, const SparkTaskInfo& taskInfo) {
    os << taskInfo.toString();
    return os;
  }
};
// 在 Apache Gluten 项目中，Runtime 类是一个核心抽象基类，它是连接 Spark JVM 层与 Native 执行引擎（如 Velox 或 ClickHouse）的会话上下文句柄。
// Runtime 的主要职责是管理单次查询任务（Spark Task）的生命周期。它的作用可以概括为以下几点：
// 引擎入口（Interface to Backend）：作为抽象层，它定义了 Native 后端必须实现的 API，如解析计划、创建迭代器、处理 Shuffle 等。
// 状态容器（State Container）：持有当前任务所需的配置（Conf）、内存管理器（MemoryManager）、Substrait 逻辑计划以及中间对象存储（ObjectStore）。
// 资源协调（Resource Coordinator）：负责将 Spark 的计算请求转换为 Native 侧的具体算子操作，例如将行数据转为列数据（Row2Columnar），或者创建数据读取/写入器。
//  任务隔离（Task Isolation）：每个 Spark 线程通常拥有一个独立的 Runtime 实例，确保不同任务之间的资源和执行上下文互不干扰。
class Runtime : public std::enable_shared_from_this<Runtime> {
 public:
  using Factory = std::function<Runtime*(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf)>;
  using Releaser = std::function<void(Runtime*)>;
  // 向全局注册表注册特定后端的创建和销毁函数。
  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);
  // 根据 kind 实例化具体的子类（如 VeloxRuntime）。
  static Runtime* create(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf = {});
  // 销毁 Runtime 实例并释放相关资源。
  static void release(Runtime*);
  static std::optional<std::string>* localWriteFilesTempPath();
  static std::optional<std::string>* localWriteFileName();

  Runtime(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& confMap)
      : kind_(kind), memoryManager_(memoryManager), confMap_(confMap) {}

  virtual ~Runtime() = default;

  virtual std::string kind() {
    return kind_;
  }
  // 将字节数组格式的 Substrait 二进制流解析为内存中的 substraitPlan_ 对象。
  virtual void parsePlan(const uint8_t* data, int32_t size) {
    throw GlutenException("Not implemented");
  }
  // 解析特定的分片信息（如 Parquet 文件路径、偏移量）。
  virtual void parseSplitInfo(const uint8_t* data, int32_t size, int32_t idx) {
    throw GlutenException("Not implemented");
  }
  // 将当前的 Native 执行计划转换为人类可读的字符串（用于 Spark 的 explain 命令）。
  virtual std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
    throw GlutenException("Not implemented");
  }
  // 获取 Substrait 计划对象的引用。
  ::substrait::Plan& getPlan() {
    return substraitPlan_;
  }
  // 最重要的函数之一
  // 根据解析好的计划创建 Native 执行树，并返回一个结果迭代器。
  virtual std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs) {
    throw GlutenException("Not implemented");
  }
  // 通知迭代器没有更多的输入分片了（通常用于流式处理或扫描结束）。
  virtual void noMoreSplits(ResultIterator* iter) {
    throw GlutenException("Not implemented");
  }
  // 用于分布式计算中的屏障同步请求。
  virtual void requestBarrier(ResultIterator* iter) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch>, const std::vector<int32_t>&) {
    throw GlutenException("Not implemented");
  }

  virtual MemoryManager* memoryManager() {
    return memoryManager_;
  };

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row.
  // 创建列转行转换器，用于将 Native 输出的向量数据转为 Spark 能够识别的 UnsafeRow 格式。
  virtual std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) {
    throw GlutenException("Not implemented");
  }
  // 建行转列转换器，用于处理 Spark 侧传入的行式数据。
  virtual std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }
  // 创建 Shuffle 写入器，负责将计算结果根据分区逻辑序列化并分发。
  virtual std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options) {
    throw GlutenException("Not implemented");
  }
  // 获取算子的执行统计指标（如处理行数、CPU 时间、峰值内存等）。
  virtual Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) {
    throw GlutenException("Not implemented");
  }
  // 创建 Shuffle 读取器，用于读取上游 Stage 产生的 Shuffle 数据。
  virtual std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) {
    throw GlutenException("Not implemented");
  }

  virtual std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }

  const std::unordered_map<std::string, std::string>& getConfMap() {
    return confMap_;
  }
  // 注入当前的 Spark 任务上下文信息。
  virtual void setSparkTaskInfo(SparkTaskInfo taskInfo) {
    taskInfo_ = taskInfo;
  }

  std::optional<SparkTaskInfo> getSparkTaskInfo() const {
    return taskInfo_;
  }

  virtual void enableDumping() {
    throw GlutenException("Not implemented");
  }

  virtual WholeStageDumper* getDumper() {
    return dumper_.get();
  }
  // 将 C++ 指针存入 ObjectStore 并返回一个唯一的 Handle。
  ObjectHandle saveObject(std::shared_ptr<void> obj) {
    return objStore_->save(obj);
  }

 protected:
  // 后端标识符（如 "velox" 或 "ch"），用于区分不同的执行后端。
  std::string kind_;
  // 内存管理器指针。关联到该任务的内存池，负责所有计算过程中的内存申请与追踪。
  MemoryManager* memoryManager_;
  // 对象存储库。用于在 Native 侧暂存中间 C++ 对象，返回一个 Handle 给 Java 侧，实现跨语言的对象引用。
  std::unique_ptr<ObjectStore> objStore_ = ObjectStore::create();
  // 会话配置映射。存储来自 Spark Session 的配置参数。
  std::unordered_map<std::string, std::string> confMap_; // Session conf map
  // Substrait 计划对象。存储从 Java 端反序列化而来的逻辑计划。
  ::substrait::Plan substraitPlan_;
  // 本地文件信息。存储该任务需要读取的数据分片（Splits/Files）信息。
  std::vector<::substrait::ReadRel_LocalFiles> localFiles_;
  // Spark 任务元数据。包含 Stage ID、Partition ID、Task ID 等，用于日志追踪和指标汇报。
  std::optional<SparkTaskInfo> taskInfo_{std::nullopt};
  // 计划导出器。用于在调试模式下将整个执行计划树导出为文本或图形。
  std::shared_ptr<WholeStageDumper> dumper_{nullptr};
};
} // namespace gluten
