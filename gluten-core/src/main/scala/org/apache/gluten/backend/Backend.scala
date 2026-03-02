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
package org.apache.gluten.backend

import org.apache.gluten.component.Component

// 在 Apache Gluten 的架构中，Backend 是一个非常基础且核心的接口。它继承自 Component（组件），定义了 Gluten 执行引擎后端的通用行为规范（例如 Velox 后端、ClickHouse 后端等）。
// Backend 特质（Trait）代表了 Gluten 的具体计算实现层。它的主要作用包括：
// 作为后端的统一抽象：它是所有具体计算后端（如 VeloxBackend、CHBackend）的基类。通过这个统一的接口，Gluten 的核心框架（gluten-core）可以不感知具体的底层实现，实现逻辑与算子执行的解耦。
// 标记为根组件（Root Component）：在 Gluten 的组件化体系中，Backend 被定义为最底层的基石。所有的规则注入、算子转换、以及 Native 库的加载，往往都起始于 Backend。
// 定义组件依赖关系的终点：它是组件依赖图（DAG）中的叶子节点（或起始节点），确保后端作为基础优先被加载。
trait Backend extends Component {

  /**
   * Backends don't have dependencies. They are all considered root components in the component DAG
   * and will be loaded at the beginning.
   */
  // 该方法用于声明当前组件依赖哪些其他组件。返回 Nil 意味着 Backend 不依赖任何其他组件。
  final override def dependencies(): Seq[Class[_ <: Component]] = Nil
}
