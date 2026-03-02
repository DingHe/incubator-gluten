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
package org.apache.gluten.substrait.extensions;

import io.substrait.proto.SimpleExtensionDeclaration;

import java.io.Serializable;

// 在 Apache Gluten 项目中，FunctionMappingNode 是 Java 层（Spark）与 Native 层（如 Velox/ClickHouse）之间进行语义对接的关键桥梁类。
// 它负责在 Java 端维护 Substrait 协议中的函数映射关系。
// 这个类的核心作用是：在内存中保存一个函数名与数字 ID 的对应关系，并能将其序列化为 Substrait 的 Protobuf 格式。
// 由于 Substrait 计划在传输时为了节省空间并提高解析效率，在算子树（RelNode）内部不直接使用字符串（如 "add", "is_not_null"），而是使用数字 ID。FunctionMappingNode 就像是字典里的一个条目：
// 它告诉 Native 引擎：“当你在执行计划里看到 ID 为 5 的函数时，它代表的就是 add 函数”。
public class FunctionMappingNode implements Serializable {
  // 存储函数的标准名称。
  // 通常是 Substrait 定义的 YAML 文件中的函数签名名称。例如，对于整数加法，它可能是 add:i32_i32。这个字符串是 Native 引擎用来查找具体 C++ 函数实现的唯一标识。
  private final String name;
  // 存储该函数在当前 Substrait 计划（Plan）中的唯一数字 ID（Anchor）。
  private final Long functionId;

  public FunctionMappingNode(String name, Long functionId) {
    this.name = name;
    this.functionId = functionId;
  }

  public SimpleExtensionDeclaration toProtobuf() {
    SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder();
    funcBuilder.setFunctionAnchor(functionId.intValue());
    funcBuilder.setName(name);

    SimpleExtensionDeclaration.Builder declaration = SimpleExtensionDeclaration.newBuilder();
    declaration.setExtensionFunction(funcBuilder.build());
    return declaration.build();
  }
}
