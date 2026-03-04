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

#include "Runtime.h"
#include "utils/Registry.h"

namespace gluten {
namespace {

// 返回一个存储 Runtime::Factory（函数指针/对象）的注册表。
// 这决定了系统如何根据后端名称（如 "velox"）创建对应的 Runtime 实例。
Registry<Runtime::Factory>& runtimeFactories() {
  static Registry<Runtime::Factory> registry;
  return registry;
}
// 返回一个存储 Runtime::Releaser 的注册表。这决定了当任务结束时，如何安全地销毁对应的 Runtime 实例。
Registry<Runtime::Releaser>& runtimeReleasers() {
  static Registry<Runtime::Releaser> registry;
  return registry;
}

} // namespace
// 作用是将特定执行引擎（如 Velox 或 ClickHouse）的“创建工厂”和“销毁函数”正式挂载到全局注册表上。
// kind: 后端的标识符字符串。例如 "velox" 代表 Velox 引擎，"ch" 代表 ClickHouse 引擎。
// factory: 一个函数对象（通常是 std::function），定义了如何实例化一个具体的 Runtime 子类。
// releaser: 一个函数对象，定义了如何安全地销毁该 Runtime 实例。
void Runtime::registerFactory(const std::string& kind, Runtime::Factory factory, Runtime::Releaser releaser) {
  runtimeFactories().registerObj(kind, std::move(factory));
  runtimeReleasers().registerObj(kind, std::move(releaser));
}

Runtime* Runtime::create(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto& factory = runtimeFactories().get(kind);
  return factory(kind, std::move(memoryManager), sessionConf);
}

void Runtime::release(Runtime* runtime) {
  const std::string kind = runtime->kind();
  auto& releaser = runtimeReleasers().get(kind);
  releaser(runtime);
}

std::optional<std::string>* Runtime::localWriteFilesTempPath() {
  // This is thread-local to conform to Java side ColumnarWriteFilesExec's design.
  // FIXME: Pass the path through relevant member functions.
  static thread_local std::optional<std::string> path;
  return &path;
}

std::optional<std::string>* Runtime::localWriteFileName() {
  // This is thread-local to conform to Java side ColumnarWriteFilesExec's design.
  // FIXME: Pass the path through relevant member functions.
  static thread_local std::optional<std::string> fileName;
  return &fileName;
}

} // namespace gluten
