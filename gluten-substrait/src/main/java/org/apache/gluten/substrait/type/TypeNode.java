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
package org.apache.gluten.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

// TypeNode 是 Java 层用于描述数据类型的核心抽象基类。它作为 Spark DataType 到 Substrait Type 协议之间的转换桥梁。
// TypeNode 的主要作用是在 Java 内存中构建一套类型描述体系。
// 由于 Gluten 需要将 Spark 的物理执行计划序列化为 Substrait 协议格式，而直接操作 Protobuf 生成的 io.substrait.proto.Type 类非常繁琐且不具备面向对象的灵活性，因此 Gluten 设计了 TypeNode 及其子类：
public abstract class TypeNode implements Serializable {
  // 标记该数据类型是否允许包含 null 值。
  protected final Boolean nullable;

  protected TypeNode(Boolean nullable) {
    this.nullable = nullable;
  }

  public abstract Type toProtobuf();

  public Boolean nullable() {
    return nullable;
  }
}
