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
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// 在 Apache Gluten 项目中，ReadRelNode 是 Substrait 算子树的起始节点，它代表了数据源扫描操作（Scan）。无论数据来自 Parquet、ORC 文件，还是 Kafka 流，都会通过这个类进行描述并转换。
// ReadRelNode 的核心作用是将 Spark 的数据读取算子（如 FileSourceScanExec 或 BatchScanExec）转换为 Substrait 协议中的 ReadRel (Read Relation)。
// 它是整个查询计划的“水源”，负责定义：
// 数据结构：读取哪些列，每列叫什么名字，是什么类型。
// 过滤下推：哪些谓词（Predicate）可以在扫描阶段就地过滤（Push-down Filter），减少传输数据量。
// 物理属性：这些列是普通数据、分区列还是元数据。
// 扩展配置：针对特定存储后端（如 Velox 的文件读取配置）的特殊参数。
public class ReadRelNode implements RelNode, Serializable {
  // 存储每一列的物理类型节点。它决定了 Native 引擎在内存中为每一列分配什么样的 Buffer。
  private final List<TypeNode> types = new ArrayList<>();
  // 存储每一列的逻辑名称。这些名称按 DFS（深度优先）顺序排列，用于构建整个计划的列标识。
  private final List<String> names = new ArrayList<>();
  // 存储列的属性分类（如 NORMAL_COL 数据列、PARTITION_COL 分区列）。这告诉后端如何获取该列的数据。
  private final List<ColumnTypeNode> columnTypeNodes = new ArrayList<>();
  // 代表下推到扫描阶段的过滤条件（如 age > 18）。如果为 null，表示全表扫描；如果不为 null，后端会在读取原始数据后立即应用此过滤逻辑。
  private final ExpressionNode filterNode;
  // 存储自定义扩展信息。常用于传递特定的文件格式参数（如 Parquet 扫描偏移量）或运行时过滤器（Runtime Filters）。
  private final AdvancedExtensionNode extensionNode;
  // 一个布尔标记，指示当前读取任务是否属于 Kafka 实时流处理。这是为了适配 Substrait 对流式数据源的扩展支持。
  private boolean streamKafka = false;

  ReadRelNode(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filterNode,
      List<ColumnTypeNode> columnTypeNodes,
      AdvancedExtensionNode extensionNode) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.filterNode = filterNode;
    this.columnTypeNodes.addAll(columnTypeNodes);
    this.extensionNode = extensionNode;
  }

  public void setStreamKafka(boolean streamKafka) {
    this.streamKafka = streamKafka;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    NamedStruct.Builder nStructBuilder =
        SubstraitUtil.createNameStructBuilder(types, names, columnTypeNodes);

    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setCommon(relCommonBuilder.build());
    readBuilder.setBaseSchema(nStructBuilder.build());
    readBuilder.setStreamKafka(streamKafka);

    if (filterNode != null) {
      readBuilder.setFilter(filterNode.toProtobuf());
    }

    if (extensionNode != null) {
      readBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }
}
