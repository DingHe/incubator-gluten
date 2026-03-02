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
package org.apache.gluten.substrait.plan;

import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.FunctionMappingNode;
import org.apache.gluten.substrait.rel.RelNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.RelRoot;

import java.io.Serializable;
import java.util.List;

// 在 Apache Gluten 项目中，PlanNode 是顶层的容器类，它代表了一整个 Substrait 执行计划（Substrait Plan）。
// 它是 Java 层构建的最后一步，也是发送给 Native 引擎之前的最终封装。
// PlanNode 的核心作用是将之前零散的函数映射、算子树（RelNode）、Schema 定义等信息组装成一个完整的、符合 Substrait 协议的二进制消息。
// 如果把查询计划比作一本书：
// RelNode 是书中的章节（具体的计算逻辑）。
// FunctionMappingNode 是书前的术语表（函数 ID 与名称的对照）。
// PlanNode 就是整本书的封面和装订，它把所有内容整合在一起，形成一个后端引擎（如 Velox）能够直接阅读和执行的实体。
public class PlanNode implements Serializable {
  // 存储该计划中用到的所有自定义函数映射。
  // 对应 Substrait 的 extensions 部分。它将计划中使用的数字 ID（Anchor）关联到具体的函数签名字符串。
  private final List<FunctionMappingNode> mappingNodes;
  // 存储执行计划中的根算子节点。
  // 虽然在 Spark 中通常只有一个根节点（如最后一个 Project 或 Aggregate），但 Substrait 协议支持在一个计划中包含多个独立的关系树，因此这里使用 List。
  private final List<RelNode> relNodes;
  // 存储最终输出结果集的列名列表。
  // 这些名称用于最终结果的呈现，确保 Native 端输出的数据列名与 Spark SQL 的预期一致。
  private final List<String> outNames;
  // 定义整个计划的最终输出模式（Schema）。
  // 它描述了输出数据的物理类型结构（如字段类型、是否可为空）。这有助于后端在输出数据前进行最终的类型校验。
  private TypeNode outputSchema = null;
  // 存储计划级别的自定义扩展信息。
  private AdvancedExtensionNode extension = null;

  PlanNode(
      List<FunctionMappingNode> mappingNodes,
      List<RelNode> relNodes,
      List<String> outNames,
      TypeNode outputSchema,
      AdvancedExtensionNode extension) {
    this.mappingNodes = mappingNodes;
    this.relNodes = relNodes;
    this.outNames = outNames;
    this.outputSchema = outputSchema;
    this.extension = extension;
  }

  public Plan toProtobuf() {
    Plan.Builder planBuilder = Plan.newBuilder();
    // add the extension functions
    for (FunctionMappingNode mappingNode : mappingNodes) {
      planBuilder.addExtensions(mappingNode.toProtobuf());
    }

    for (RelNode relNode : relNodes) {
      PlanRel.Builder planRelBuilder = PlanRel.newBuilder();

      RelRoot.Builder relRootBuilder = RelRoot.newBuilder();
      relRootBuilder.setInput(relNode.toProtobuf());
      for (String name : outNames) {
        relRootBuilder.addNames(name);
      }
      if (outputSchema != null) {
        relRootBuilder.setOutputSchema(outputSchema.toProtobuf().getStruct());
      }
      planRelBuilder.setRoot(relRootBuilder.build());

      planBuilder.addRelations(planRelBuilder.build());
    }

    if (extension != null) {
      planBuilder.setAdvancedExtensions(extension.toProtobuf());
    }
    return planBuilder.build();
  }
}
