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

import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.ExtensionBuilder;
import org.apache.gluten.substrait.extensions.FunctionMappingNode;
import org.apache.gluten.substrait.rel.RelNode;
import org.apache.gluten.substrait.type.TypeNode;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
// 在 Apache Gluten 项目中，PlanBuilder 是一个核心的工厂类（Factory Class），主要负责构建 Substrait Plan。
// Substrait 是一种跨语言的、标准化的关系代数（Relational Algebra）表示格式。
// Gluten 的核心逻辑就是将 Spark 的物理计划（SparkPlan）转换为 Substrait 计划，然后传递给底层原生后端（如 Velox、ClickHouse）执行。
// PlanBuilder 的主要作用是封装 Substrait 计划（Plan）的组装逻辑。
// 一个完整的 Substrait 计划不仅包含算子树（如 Filter、Project、Join），还包含元数据信息。PlanBuilder 负责收集这些零散的组件：
// 函数映射（Function Mapping）：记录计划中使用的所有标量函数、聚合函数及其 ID。
// 关系节点（Relational Nodes/RelNodes）：实际的执行算子流。
// 输出模式（Output Schema）：定义结果集的字段名称和数据类型
// 高级扩展（Extensions）：用于传递 Spark 特有的、Substrait 标准之外的自定义信息。
public class PlanBuilder {
  // 定义一个“空计划”的字节数组。
  public static byte[] EMPTY_PLAN = empty().toProtobuf().toByteArray();
  // 防止外部实例化。这是一个典型的工具类（Utility Class）设计，所有功能都通过静态方法提供。
  private PlanBuilder() {}
  // 最底层的构造方法，直接创建一个 PlanNode 对象。
  public static PlanNode makePlan(
      List<FunctionMappingNode> mappingNodes,
      List<RelNode> relNodes,
      List<String> outNames,
      TypeNode outputSchema,
      AdvancedExtensionNode extension) {
    return new PlanNode(mappingNodes, relNodes, outNames, outputSchema, extension);
  }
  // 业务组装核心
  public static PlanNode makePlan(
      SubstraitContext subCtx, List<RelNode> relNodes, List<String> outNames) {
    return makePlan(subCtx, relNodes, outNames, null, null);
  }

  public static PlanNode makePlan(
      SubstraitContext subCtx,
      List<RelNode> relNodes,
      List<String> outNames,
      TypeNode outputSchema,
      AdvancedExtensionNode extension) {
    Preconditions.checkNotNull(
        subCtx, "Cannot execute doTransform due to the SubstraitContext is null.");
    List<FunctionMappingNode> mappingNodes = new ArrayList<>();

    for (Map.Entry<String, Long> entry : subCtx.registeredFunction().entrySet()) {
      FunctionMappingNode mappingNode =
          ExtensionBuilder.makeFunctionMapping(entry.getKey(), entry.getValue());
      mappingNodes.add(mappingNode);
    }
    return makePlan(mappingNodes, relNodes, outNames, outputSchema, extension);
  }

  public static PlanNode makePlan(SubstraitContext subCtx, ArrayList<RelNode> relNodes) {
    return makePlan(subCtx, relNodes, new ArrayList<>());
  }

  public static PlanNode empty() {
    return makePlan(new SubstraitContext(), new ArrayList<>(), new ArrayList<>());
  }
}
