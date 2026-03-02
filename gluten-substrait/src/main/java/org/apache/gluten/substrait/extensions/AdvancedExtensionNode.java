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

import com.google.protobuf.Any;
import io.substrait.proto.AdvancedExtension;

import java.io.Serializable;

// 在 Apache Gluten 项目中，AdvancedExtensionNode 是 Substrait 协议中用于处理自定义扩展逻辑的顶级容器。
// 它允许 Gluten 在标准的 SQL 算子（如 Join, Agg）之外，向 Native 后端（如 Velox）传递特定于 Spark 或特定于引擎的元数据。
// 这个类的核心作用是提供一个“逃生舱（Escape Hatch）”机制，用于传递 Substrait 标准协议中尚未定义、但执行时必需的信息。
// Substrait 旨在成为通用协议，但不同的后端引擎（Velox, ClickHouse）或前端（Spark）总会有一些特殊需求。AdvancedExtensionNode 允许将这些特殊需求包装在 google.protobuf.Any 类型中，以“黑盒”形式透传给后端。
public class AdvancedExtensionNode implements Serializable {

  // An optimization is helpful information that don't influence semantics. May
  // be ignored by a consumer.
  // 优化建议（Optimization）。
  // 即使后端引擎忽略了这部分内容，查询结果依然是正确的。
  // 场景：统计信息（Statistics）、执行启发式提示（Hints）等。
  private final Any optimization;

  // An enhancement alter semantics. Cannot be ignored by a consumer.
  // 语义增强（Enhancement）。
  // 如果后端引擎不能识别或不支持这部分内容，则必须报错并停止执行。
  // 场景：自定义的聚合算法、特殊的排序规则、或者 Gluten 扩展的 Native 算子属性。
  private final Any enhancement;

  public AdvancedExtensionNode(Any enhancement) {
    this.optimization = null;
    this.enhancement = enhancement;
  }

  public AdvancedExtensionNode(Any optimization, Any enhancement) {
    this.optimization = optimization;
    this.enhancement = enhancement;
  }

  public AdvancedExtension toProtobuf() {
    AdvancedExtension.Builder extensionBuilder = AdvancedExtension.newBuilder();
    if (optimization != null) {
      extensionBuilder.setOptimization(optimization);
    }
    if (enhancement != null) {
      extensionBuilder.setEnhancement(enhancement);
    }
    return extensionBuilder.build();
  }
}
