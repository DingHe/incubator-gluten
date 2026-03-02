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
package org.apache.gluten.backendsapi

import org.apache.gluten.execution.{BaseGlutenPartition, LeafTransformSupport, WholeStageTransformContext}
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch
// IteratorApi 是 Apache Gluten 架构中负责**数据执行流（Runtime Execution）**的关键接口。
// 在 Spark 的物理计划被转换并传输到 Native 层（通过 Substrait 协议）之后，真正触发计算、并从 Native 引擎（如 Velox 或 ClickHouse）中拉取数据的逻辑，全部封装在 IteratorApi 的实现中。
// Native 迭代器适配：将 Native 引擎的执行结果包装成 Java 侧的 Iterator[ColumnarBatch]，使得 Spark 能够像处理普通迭代器一样处理 Native 计算结果。
// 桥接逻辑与物理分片：负责将 Spark 的 Partition 信息转换为 Native 引擎能理解的 SplitInfo。
// 计算流水线触发：定义了如何根据不同的执行阶段（First Stage 或 Final Stage）启动 Native 算子流水线（Pipeline）。
trait IteratorApi {
  // 将 Spark 的扫描信息转换为 Substrait 协议中的分片信息。
  // 在分布式读取文件时，该方法负责采集文件的路径、格式（fileFormat）、模式（dataSchema）、元数据列名以及其他存储属性。
  // 返回值：SplitInfo。它是 Native 引擎读取数据源（如 Parquet 或 ORC 文件）所需的最小任务描述符。
  def genSplitInfo(
      partitionIndex: Int,
      partition: Seq[Partition],
      partitionSchema: StructType,
      dataSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo

  /** Generate native row partition. */
  // 生成 Gluten 特有的分区对象。
  // wsCtx: 全阶段代码生成（WholeStageTransform）的上下文，包含了 Substrait 计划信息。
  def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      leaves: Seq[LeafTransformSupport]): Seq[BaseGlutenPartition]

  /**
   * Inject the task attempt temporary path for native write files, this method should be called
   * before `genFirstStageIterator` or `genFinalStageIterator`
   * @param path
   *   is the temporary directory for native write pipeline
   * @param fileName
   *   is the file name for native write pipeline, backend could generate it by itself.
   */
  // 为 Native 写入管道注入临时存储路径。
  // 当执行 INSERT 或 WRITE 操作时，Native 引擎需要知道中间结果写到哪个临时目录。
  // 此方法允许后端（Backend）在计算开始前设置这些路径。
  def injectWriteFilesTempPath(path: String, fileName: String): Unit =
    throw new UnsupportedOperationException()

  /**
   * Generate Iterator[ColumnarBatch] for first stage. ("first" means it does not depend on other
   * SCAN inputs)
   */
  // 生成第一阶段（数据源阶段）的 Native 迭代器。
  def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: InputMetricsWrapper => Unit,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq(),
      enableCudf: Boolean = false
  ): Iterator[ColumnarBatch]

  /**
   * Generate Iterator[ColumnarBatch] for final stage. ("Final" means it depends on other SCAN
   * inputs, maybe it was a mistake to use the word "final")
   */
  // 生成非数据源阶段（依赖前级输入）的 Native 迭代器。
  // 这里的 “Final” 并不一定指 SQL 的最后一步，而是指该阶段依赖其他 RDD 的输入（比如 Shuffle 之后的数据或多个算子合并后的阶段）
  // scalastyle:off argcount
  def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean = false,
      enableCudf: Boolean = false): Iterator[ColumnarBatch]
  // scalastyle:on argcount
}
