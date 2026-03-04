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
package org.apache.gluten.runtime;

// 在 Apache Gluten 项目中，RuntimeJniWrapper 类是 Java 层（JVM）与 C++ 层（Native）之间最基础、最重要的通信桥梁。
// 由于 Gluten 的核心执行引擎（如 Velox 或 ClickHouse）是用 C++ 编写的，而 Spark 运行在 Java 虚拟机上，因此需要 JNI (Java Native Interface) 来进行跨语言调用。
// RuntimeJniWrapper 的作用可以概括为：
// 句柄管理中心：它负责触发 Native 侧 Runtime 对象的创建和销毁，并将 C++ 对象的内存地址以 long 类型的句柄（Handle）形式返回给 Java 侧。
// 后端环境初始化：通过调用此类的方法，Java 侧可以要求 Native 侧根据指定的后端类型（如 "velox"）准备好执行环境。
// 生命周期入口：它是每个查询任务（Task）在进入本地执行阶段之前的“第一站”。
public class RuntimeJniWrapper {
  // 私有构造函数。
  // 详述：防止该类被实例化。因为 JNI 包装类通常只需要提供静态方法供 Java 层直接调用，不需要创建 Java 对象。
  private RuntimeJniWrapper() {}
  // 在 C++ 侧创建一个新的 Runtime 实例。
  // String backendType：后端类型字符串。通常传入 "velox" 或 "ch"。它决定了 Native 侧会通过哪个工厂类来实例化具体的运行时环境。
  public static native long createRuntime(String backendType, long nmm, byte[] sessionConf);

  public static native void releaseRuntime(long handle);
}
