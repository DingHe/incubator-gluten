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

import io.substrait.proto.NamedStruct.ColumnType;

import java.io.Serializable;
// ColumnTypeNode 是 Java 层对 Substrait 协议中 列属性类型（ColumnType） 的简单包装。
// 这个类的核心作用是：在 Java 端标识并传递数据列的“物理性质”。
// 在分布式文件系统（如 HDFS/S3）中，一个表的列并不全都在数据文件里。有些列是来自文件路径的分区信息，有些是系统自动生成的元数据。
// ColumnTypeNode 负责将这些分类信息从 Spark 端的扫描算子（Scan）包装好，传递给 Substrait 计划，最终告知 Native 后端（如 Velox）该如何获取这些数据。
public class ColumnTypeNode implements Serializable {

  private final ColumnType columnType;

  public ColumnTypeNode(ColumnType columnType) {
    this.columnType = columnType;
  }

  public ColumnType toProtobuf() {
    return columnType;
  }
}
