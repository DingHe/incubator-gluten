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
package org.apache.gluten.substrait.expression;

import io.substrait.proto.Expression;

/** Contains helper functions for constructing Substrait expressions. */
// 在 Apache Gluten 项目中，ExpressionNode 是 Java 层对接 Substrait 协议的一个核心抽象接口。它为所有类型的计算逻辑（表达式）定义了统一的转换规范。
// ExpressionNode 的主要作用是作为 Spark 表达式与 Substrait 二进制协议之间的中转抽象。
// 承上（对接 Spark）：Gluten 会遍历 Spark 的表达式树（Catalyst Expressions，如 Add, Substring, CaseWhen），并将它们转换成对应的 Java ExpressionNode 实现类。
// 启下（对接 Native）：这些节点最终需要被序列化为 Protobuf 格式，才能通过 JNI 发送到 C++ 后端（如 Velox）。ExpressionNode 确保了无论多么复杂的表达式，都能以统一的方式触发这个序列化过程。
// 核心意义： 它使得 Gluten 能够以面向对象的方式在 Java 端构建、修改和校验表达式树，而无需直接处理极其繁琐的 Protobuf 生成代码。
public interface ExpressionNode {
  /**
   * Converts a Expression into a protobuf.
   *
   * @return A rel protobuf
   */
  Expression toProtobuf();
}
