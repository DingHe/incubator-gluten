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

import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;

import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
// 在 Apache Gluten 项目中，ProjectRelNode 是对 Substrait 协议中 投影算子（Project Relation） 的封装。它对应 SQL 中的 SELECT 部分，负责对数据进行字段选择或表达式计算。
// ProjectRelNode 的核心作用是将 Spark 的投影操作（如 ProjectExec）转换为 Substrait 的物理计划节点。
// 计算与转换：基于输入数据，计算一系列表达式（如 col_a + col_b，或字符串截取 substring(col_c)）。
// 结果输出：将计算后的新字段作为输出。Substrait 的 Project 默认采用“追加模式”，即输出 = 输入的所有字段 + 新计算的表达式字段。
// 字段映射（Emit）：通过控制 Emit 机制，决定最终向下游算子传递哪些字段，从而实现字段的重新排序或修剪。
public class ProjectRelNode implements RelNode, Serializable {
  // 该投影算子的输入节点。
  // 代表了数据流的上游（例如一个 ReadRelNode 或 FilterRelNode）。在构建执行树时，它形成了父子节点的递归关系。
  private final RelNode input;
  // 存储投影操作中需要计算的表达式列表。
  // 每一项都是一个 ExpressionNode（如加法、函数调用、常量等）。这些表达式会被应用于输入的每一行数据。
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();
  // 用于向 Native 引擎传递非标准协议的参数。如果不需要特殊扩展，通常为 null。
  private final AdvancedExtensionNode extensionNode;
  // 控制字段输出映射的起始索引。
  // 这是 Gluten 处理 Substrait 字段索引的关键。在 Substrait 规范中，Project 默认会保留所有输入列。Gluten 通过 Emit 机制来选择性输出特定范围的列。
  private final int emitStartIndex;

  ProjectRelNode(RelNode input, List<ExpressionNode> expressionNodes, int emitStartIndex) {
    this.input = input;
    this.expressionNodes.addAll(expressionNodes);
    this.extensionNode = null;
    this.emitStartIndex = emitStartIndex;
  }

  ProjectRelNode(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      AdvancedExtensionNode extensionNode,
      int emitStartIndex) {
    this.input = input;
    this.expressionNodes.addAll(expressionNodes);
    this.extensionNode = extensionNode;
    this.emitStartIndex = emitStartIndex;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    if (emitStartIndex < 0) {
      relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
    } else {
      RelCommon.Emit.Builder emitBuilder = RelCommon.Emit.newBuilder();
      for (int i = 0; i < expressionNodes.size(); i++) {
        emitBuilder.addOutputMapping(i + emitStartIndex);
      }
      relCommonBuilder.setEmit(emitBuilder.build());
    }
    ProjectRel.Builder projectBuilder = ProjectRel.newBuilder();
    projectBuilder.setCommon(relCommonBuilder.build());
    if (input != null) {
      projectBuilder.setInput(input.toProtobuf());
    }
    for (ExpressionNode expressionNode : expressionNodes) {
      projectBuilder.addExpressions(expressionNode.toProtobuf());
    }
    if (extensionNode != null) {
      projectBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setProject(projectBuilder.build());
    return builder.build();
  }
}
