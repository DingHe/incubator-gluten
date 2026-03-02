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
package org.apache.gluten.substrait.rel;

import io.substrait.proto.Rel;

import java.io.Serializable;

/** Contains helper functions for constructing substrait relations. */
// 在 Apache Gluten 项目中，RelNode 是 Java 层对接 Substrait 协议的一个核心抽象接口。它定义了所有“关系算子节点”（Relation Node）必须遵循的标准。
// RelNode 的主要作用是充当 Spark 物理算子与 Substrait Protobuf 对象之间的“中间载体”。
// 转换阶段：Gluten 遍历 Spark 的物理计划（SparkPlan），将每个算子（如 FilterExec, ProjectExec）转换成对应的 Java RelNode 实现类（如 FilterRelNode, ProjectRelNode）。
// 树构建：这些 RelNode 相互嵌套，在内存中构建出一棵逻辑上的算子树。
// 序列化阶段：当整棵树构建完成后，通过调用 RelNode 定义的方法，将其整体转换为 Substrait 的二进制 Protobuf 格式，最终通过 JNI 发送给 C++ 后端执行。
// 多态处理：允许 PlanBuilder 在组装计划时，不需要关心具体的算子类型，只要它是 RelNode 即可。
public interface RelNode extends Serializable {
  /**
   * Converts a Rel into a protobuf.
   *
   * @return A rel protobuf
   */
  // 这是该接口最核心的方法。它负责将当前的 Java RelNode 对象转换成 Substrait 协议定义的底层 Protobuf 消息。
  Rel toProtobuf();
}
