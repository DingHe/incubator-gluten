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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <filesystem>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/MmapAllocator.h"

#include "memory/VeloxMemoryManager.h"

namespace gluten {

// This kind string must be same with VeloxBackend#name in java side.
inline static const std::string kVeloxBackendKind{"velox"};
/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
// 在 Apache Gluten 项目中，VeloxBackend 类是一个至关重要的单例类。它是 Gluten C++ 侧（即 Velox 运行时）的全局控制中心。
// VeloxBackend 的核心使命是生命周期管理和全局资源配置。它的主要作用包括：
// 执行器级单例：在每个 Spark Executor 进程中，VeloxBackend 仅存在一个实例，伴随整个进程的生命周期。
// 桥接 Java 与 C++：它接收来自 Java 侧的配置信息（Spark Conf），并将其转化为 Velox 引擎可理解的参数。
// 全局资源初始化：它是 Velox 引擎启动的入口，负责初始化内存管理器（Memory Manager）、缓存（Cache）、文件系统连接器（Connector）和用户自定义函数（UDF）。
// 共享资源池：为该进程内的所有查询任务（Tasks）提供共享的异步数据缓存（AsyncDataCache）和 IO 线程池。
class VeloxBackend {
 public:
  ~VeloxBackend() {}
  // 初始化单例实例。
  // 参数：listener 用于监听内存分配事件（通常用于向 Spark 汇报内存使用情况）；conf 是从 Java 传入的键值对配置图。
  static void create(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf);
  // 获取 VeloxBackend 的单例指针。
  static VeloxBackend* get();
  // 获取异步数据缓存指针，供 Scan 算子使用。
  facebook::velox::cache::AsyncDataCache* getAsyncDataCache() const;
  // 获取全局配置对象。
  std::shared_ptr<facebook::velox::config::ConfigBase> getBackendConf() const {
    return backendConf_;
  }
  // 获取内存管理器，用于创建算子级别的子内存池（Child Memory Pools）
  VeloxMemoryManager* getGlobalMemoryManager() const {
    return globalMemoryManager_.get();
  }
  // 资源清理。在 Executor 退出时被调用，释放缓存、关闭线程池并销毁实例。
  void tearDown();

 private:
  explicit VeloxBackend(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf) {
    init(std::move(listener), conf);
  }
  // 作用：核心初始化序列。按顺序调用以下各个私有 init 方法，并设置内存管理规则。
  void init(std::unique_ptr<AllocationListener> listener, const std::unordered_map<std::string, std::string>& conf);
  // 初始化内存缓存和（可选的）SSD 缓存。它会根据配置决定分配多少 GB 的内存给 AsyncDataCache。
  void initCache();
  // 注册 Velox 连接器。通常会注册 HiveConnector（用于处理 Parquet/ORC），设置文件系统（如 HDFS, S3, Local）的相关参数。
  void initConnector(const std::shared_ptr<facebook::velox::config::ConfigBase>& hiveConf);
  // 注册全局 UDF（用户自定义函数）。Gluten 会在这里加载内置的标量函数和聚合函数映射。
  void initUdf();
  // 作用：如果开启了 SSD 缓存，该方法会配置 SSD 的路径、空间上限和线程池。
  std::unique_ptr<facebook::velox::cache::SsdCache> initSsdCache(uint64_t ssdSize);
  // 初始化特殊的物理层文件系统支持（JOL 通常指一种特定的文件访问抽象）。
  void initJolFilesystem();
  // 生成一个基于 UUID 的随机字符串。
  std::string getCacheFilePrefix() {
    return "cache." + boost::lexical_cast<std::string>(boost::uuids::random_generator()()) + ".";
  }
  // 存储全局唯一的单例实例。
  static std::unique_ptr<VeloxBackend> instance_;

  // A global Velox memory manager for the current process.
  // 全局内存管理器。
  // 负责管理整个进程的堆外内存。它会与 Velox 的内存池（MemoryPool）系统对接。
  std::unique_ptr<VeloxMemoryManager> globalMemoryManager_;
  // Instance of AsyncDataCache used for all large allocations.
  // 异步数据缓存。Velox 的高性能缓存层。
  // 用于缓存从 HDFS/S3 等读取的文件数据片段，减少重复 IO。
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> asyncDataCache_;
  // SSD 缓存线程池。专门用于处理 SSD 缓存读写的 folly 线程池，确保缓存 IO 不阻塞计算。
  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  // 全局 IO 线程池。负责处理通用的异步文件读取任务。
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  // 内存映射分配器。一种特殊的内存分配器，通常用于管理大块缓存空间，优化内存页的分配和收回。
  std::shared_ptr<facebook::velox::memory::MmapAllocator> cacheAllocator_;
  // 缓存文件的路径前缀（例如 /tmp/gluten_cache/）。
  std::string cachePathPrefix_;
  // 缓存文件的随机名称前缀，防止不同执行器间的缓存文件冲突。
  std::string cacheFilePrefix_;
  // 后端配置对象。存储经过解析后的 Velox 专用配置参数。
  std::shared_ptr<facebook::velox::config::ConfigBase> backendConf_;
};

} // namespace gluten
