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
#include <filesystem>

#include "VeloxBackend.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_GPU
#include "operators/plannodes/CudfVectorStream.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#endif

#include "compute/VeloxRuntime.h"
#include "config/VeloxConfig.h"
#include "jni/JniFileSystem.h"
#include "memory/GlutenBufferedInputBuilder.h"
#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "shuffle/ArrowShuffleDictionaryWriter.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_ssd_odirect);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_velox_async_timeout_on_task_stopping);
DEFINE_int32(gluten_velox_async_timeout_on_task_stopping, 30000, "Async timout when task is being stopped");

using namespace facebook;

namespace gluten {

namespace {
// 作用是根据给定的参数创建一个 VeloxMemoryManager 实例。
// MemoryManager*: 函数返回一个指向 MemoryManager 基类的指针。这体现了多态性，因为 VeloxMemoryManager 是其子类。
// const std::string& kind: 输入参数，表示内存管理器的类型（在 Velox 后端中通常传入字符串 "velox"）。
// std::unique_ptr<AllocationListener> listener: 传入一个独占指针，指向内存分配监听器。
// 这个监听器非常重要，它负责将 C++ 侧（Velox）分配的内存变化实时通知给 Java 侧（Spark），以便 Spark 能够进行内存配额管理。
MemoryManager* veloxMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new VeloxMemoryManager(kind, std::move(listener), *VeloxBackend::get()->getBackendConf());
}

void veloxMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}
// 作用是实例化一个 Velox 运行环境（VeloxRuntime）。
// Runtime 是 Gluten 中最核心的对象之一，它封装了执行单次查询任务（Task）所需的所有上下文信息
// Runtime*: 返回指向基类 Runtime 的指针。这是 Gluten 插件化架构的体现，允许 Spark 动态加载不同的后端（如 Velox 或 ClickHouse）。
// const std::string& kind: 后端类型的名称，对于 Velox 后端，通常传入 "velox"。
// MemoryManager* memoryManager: 传入一个通用的内存管理器指针。这是上一步 veloxMemoryManagerFactory 创建的对象。
Runtime* veloxRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto* vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return new VeloxRuntime(kind, vmm, sessionConf);
}

void veloxRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}
} // namespace
// 核心初始化函数 init。
// 它负责在 Executor（执行器）启动时，将 Velox 引擎的所有全局组件（配置、内存、IO、文件系统、算子等）配置并注册到位。
void VeloxBackend::init(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  // 将传入的 conf（来自 Spark）保存为 Velox 专用的 ConfigBase 对象。
  backendConf_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(conf));
  // 实例化全局内存管理器。这是 Gluten C++ 侧管理堆外内存的起点。
  globalMemoryManager_ = std::make_unique<VeloxMemoryManager>(kVeloxBackendKind, std::move(listener), *backendConf_);

  // Register factories.
  // 将我们之前分析过的 veloxMemoryManagerFactory 和 veloxRuntimeFactory 注册到 Gluten 的工厂注册表中。这样当 Java 端请求创建内存或运行时环境时，系统知道该调用哪些 Velox 专用的函数。
  MemoryManager::registerFactory(kVeloxBackendKind, veloxMemoryManagerFactory, veloxMemoryManagerReleaser);
  Runtime::registerFactory(kVeloxBackendKind, veloxRuntimeFactory, veloxRuntimeReleaser);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(backendConf_->rawConfigs());
  }

  // Init glog and log level.
  // 初始化 Google 的 glog 库。
  if (!backendConf_->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = backendConf_->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (backendConf_->valueExists(kGlogVerboseLevel)) {
      FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    } else {
      FLAGS_v = kGlogVerboseLevelMaximum;
    }
  }
  // FLAGS_logtostderr = true：强制将日志输出到标准错误流，以便 Spark 能够捕获 Native 侧的日志。
  FLAGS_logtostderr = true;
  google::InitGoogleLogging("gluten");

  // Allow growing buffer in another task through its memory pool.
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks =
      backendConf_->get<bool>(kMemoryPoolCapacityTransferAcrossTasks, true);

  // Avoid creating too many shared leaf pools.
  FLAGS_velox_memory_num_shared_leaf_pools = 0;

  // Set velox_exception_user_stacktrace_enabled.
  FLAGS_velox_exception_user_stacktrace_enabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  FLAGS_velox_exception_system_stacktrace_enabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  FLAGS_velox_memory_use_hugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_velox_async_timeout_on_task_stopping =
      backendConf_->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);

  // Set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  auto hiveConf = createHiveConnectorConfig(backendConf_);

  // Setup and register.
  // 注册本地文件系统
  velox::filesystems::registerLocalFileSystem();
  // 条件编译注册 (HDFS/S3/GCS/ABFS)：根据编译宏，启用对分布式文件系统的支持。例如，如果开启了 ENABLE_S3，Velox 就能通过 s3:// 协议读取数据。
#ifdef ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif
#ifdef ENABLE_S3
  velox::filesystems::registerS3FileSystem();
#endif
#ifdef ENABLE_GCS
  velox::filesystems::registerGcsFileSystem();
#endif
#ifdef ENABLE_ABFS
  velox::filesystems::registerAbfsFileSystem();
  velox::filesystems::registerAzureClientProvider(*hiveConf);
#endif

#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    std::unordered_map<std::string, std::string> options = {
        {velox::cudf_velox::CudfConfig::kCudfEnabled, "true"},
        {velox::cudf_velox::CudfConfig::kCudfDebugEnabled, backendConf_->get(kDebugCudf, kDebugCudfDefault)},
        {velox::cudf_velox::CudfConfig::kCudfMemoryResource,
         backendConf_->get(kCudfMemoryResource, kCudfMemoryResourceDefault)},
        {velox::cudf_velox::CudfConfig::kCudfMemoryPercent,
         backendConf_->get(kCudfMemoryPercent, kCudfMemoryPercentDefault)}};
    auto& cudfConfig = velox::cudf_velox::CudfConfig::getInstance();
    cudfConfig.initialize(std::move(options));
    velox::cudf_velox::registerCudf();
    velox::exec::Operator::registerOperator(std::make_unique<CudfVectorStreamOperatorTranslator>());
  }
#endif

  initJolFilesystem();
  // 注册 Hive 连接器，这是处理 Spark SQL 任务（Parquet/ORC 文件）的核心。
  initConnector(hiveConf);

  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::orc::registerOrcReaderFactory();
  velox::exec::ExprToSubfieldFilterParser::registerParser(std::make_unique<SparkExprToSubfieldFilterParser>());
  velox::connector::hive::BufferedInputBuilder::registerBuilder(std::make_shared<GlutenBufferedInputBuilder>());

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(facebook::velox::VectorSerde::Kind::kPresto)) {
    // RSS shuffle serde.
    facebook::velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  initUdf();

  // Initialize Velox-side memory manager for current process. The memory manager
  // will be used during spill calls so we don't track it with Spark off-heap memory instead
  // we rely on overhead memory. If we track it with off-heap memory, recursive reservations from
  // Spark off-heap memory pool will be conducted to cause unexpected OOMs.
  auto sparkOverhead = backendConf_->get<int64_t>(kSparkOverheadMemory);
  int64_t memoryManagerCapacity;
  if (sparkOverhead.has_value()) {
    // 0.75 * total overhead memory is used for Velox global memory manager.
    // FIXME: Make this configurable.
    memoryManagerCapacity = sparkOverhead.value() * 0.75;
  } else {
    memoryManagerCapacity = facebook::velox::memory::kMaxMemory;
  }
  LOG(INFO) << "Setting global Velox memory manager with capacity: " << memoryManagerCapacity;
  facebook::velox::memory::MemoryManager::Options options;
  options.allocatorCapacity = memoryManagerCapacity;
  facebook::velox::memory::initializeMemoryManager(options);

  // local cache persistent relies on the cache pool from root memory pool so we need to init this
  // after the memory manager instanced
  initCache();

  registerShuffleDictionaryWriterFactory([](MemoryManager* memoryManager, arrow::util::Codec* codec) {
    return std::make_unique<ArrowShuffleDictionaryWriter>(memoryManager, codec);
  });
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  registerJolFileSystem(maxSpillFileSize);
}

std::unique_ptr<facebook::velox::cache::SsdCache> VeloxBackend::initSsdCache(uint64_t ssdCacheSize) {
  FLAGS_velox_ssd_odirect = backendConf_->get<bool>(kVeloxSsdODirectEnabled, false);
  int32_t ssdCacheShards = backendConf_->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault);
  int32_t ssdCacheIOThreads = backendConf_->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault);
  std::string ssdCachePathPrefix = backendConf_->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault);
  uint64_t ssdCheckpointIntervalSize = backendConf_->get<uint64_t>(kVeloxSsdCheckpointIntervalBytes, 0);
  bool disableFileCow = backendConf_->get<bool>(kVeloxSsdDisableFileCow, false);
  bool checksumEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumEnabled, false);
  bool checksumReadVerificationEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumReadVerificationEnabled, false);

  cachePathPrefix_ = ssdCachePathPrefix;
  cacheFilePrefix_ = getCacheFilePrefix();
  std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
  ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
  const cache::SsdCache::Config config(
      ssdCachePath,
      ssdCacheSize,
      ssdCacheShards,
      ssdCacheExecutor_.get(),
      ssdCheckpointIntervalSize,
      disableFileCow,
      checksumEnabled,
      checksumReadVerificationEnabled);
  auto ssd = std::make_unique<velox::cache::SsdCache>(config);
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
  if (si.available < ssdCacheSize) {
    VELOX_FAIL(
        "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
        "free space: " + std::to_string(si.available));
  }
  LOG(INFO) << "Initializing SSD cache with: " << config.toString();
  return ssd;
}

void VeloxBackend::initCache() {
  if (backendConf_->get<bool>(kVeloxCacheEnabled, false)) {
    uint64_t memCacheSize = backendConf_->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      auto ssd = initSsdCache(ssdCacheSize);
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()));
    LOG(INFO) << "AsyncDataCache is ready";
  }
}

void VeloxBackend::initConnector(const std::shared_ptr<velox::config::ConfigBase>& hiveConf) {
  auto ioThreads = backendConf_->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);
  GLUTEN_CHECK(
      ioThreads >= 0,
      kVeloxIOThreads + " was set to negative number " + std::to_string(ioThreads) + ", this should not happen.");
  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  }
  velox::connector::registerConnector(
      std::make_shared<velox::connector::hive::HiveConnector>(kHiveConnectorId, hiveConf, ioExecutor_.get()));
  
  // Register value-stream connector for runtime iterator-based inputs
  velox::connector::registerConnector(std::make_shared<ValueStreamConnector>(kIteratorConnectorId, hiveConf));
  
#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault) &&
      backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory factory;
    auto hiveConnector = factory.newConnector(kCudfHiveConnectorId, hiveConf, ioExecutor_.get());
    facebook::velox::connector::registerConnector(hiveConnector);
  }
#endif
}

void VeloxBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kVeloxUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<VeloxBackend> VeloxBackend::instance_ = nullptr;

void VeloxBackend::create(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<VeloxBackend>(new VeloxBackend(std::move(listener), conf));
}

VeloxBackend* VeloxBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "VeloxBackend instance is null, please invoke VeloxBackend#create before use.";
    throw GlutenException("VeloxBackend instance is null.");
  }
  return instance_.get();
}

void VeloxBackend::tearDown() {
#ifdef ENABLE_HDFS
  for (const auto& [_, filesystem] : facebook::velox::filesystems::registeredFilesystems) {
    filesystem->close();
  }
#endif

  // Destruct IOThreadPoolExecutor will join all threads.
  // On threads exit, thread local variables can be constructed with referencing global variables.
  // So, we need to destruct IOThreadPoolExecutor and stop the threads before global variables get destructed.
  ioExecutor_.reset();
  globalMemoryManager_.reset();

  // dump cache stats on exit if enabled
  if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
    LOG(INFO) << asyncDataCache_->toString();
    for (const auto& entry : std::filesystem::directory_iterator(cachePathPrefix_)) {
      if (entry.path().filename().string().find(cacheFilePrefix_) != std::string::npos) {
        LOG(INFO) << "Removing cache file " << entry.path().filename().string();
        std::filesystem::remove(cachePathPrefix_ + "/" + entry.path().filename().string());
      }
    }
    asyncDataCache_->shutdown();
  }
}

} // namespace gluten
