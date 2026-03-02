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

import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import org.apache.spark.util.collection.BitSet

import com.google.protobuf.{Any, Message}

import java.util
// TransformerApi 是 Apache Gluten 架构中非常核心的转换接口，它定义了如何将 Spark 的物理算子与表达式转换成 Native 后端（通过 Substrait 协议）可识别的数据结构。
// 由于不同的 Native 引擎（如 Velox, ClickHouse）在数据读取、表达式处理和配置要求上存在差异，TransformerApi 提供了后端特有的逻辑定制点。
// 数据读取定制化：负责 File Scan 阶段的分区（Partition）生成逻辑。
// Substrait 表达映射：处理特定复杂表达式（如高精度数值处理）到 Substrait 节点的转换。
// 后端配置适配：允许后端根据 Spark 的运行环境动态调整 Native 层的运行参数。
// 序列化与协议转换：负责 Protobuf 消息的打包以及将二进制 Substrait 计划翻译为人类可读的字符串（用于 Debug）。
// 写文件支持：生成 Native 层写入文件所需的特定参数。
trait TransformerApi {

  /** Generate Seq[Partition] for FileSourceScanExecTransformer. */
  // 作用：为 FileSourceScanExecTransformer 生成 Spark 分区序列。
  // 背景：Spark 读取文件需要将文件切分成多个 Partition。Gluten 的 Native Scan 需要在兼容 Spark 分区逻辑的基础上，可能需要添加一些后端特有的元数据（如文件格式信息、列投影等）。
  def genPartitionSeq(
      relation: HadoopFsRelation,
      requiredSchema: StructType,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression] = Seq.empty): Seq[Partition]

  /**
   * Post-process native config, For example, for ClickHouse backend, sync 'spark.executor.cores' to
   * 'spark.gluten.sql.columnar.backend.ch.runtime_settings.max_threads'
   */
  // 后处理 Native 配置。
  // 允许后端将 Spark 层的配置“同步”或“转换”为 Native 层的参数。
  def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {}
  // 获取后端支持的表达式类名集合
  // 用于在算子转换前进行校验，确定哪些 Spark 表达式可以安全地 Offload（下推）到 Native 层。
  def getSupportExpressionClassName: util.Set[String] = {
    util.Collections.emptySet()
  }
  // 获取物理计划的输出属性。
  // 默认直接返回 plan.output。但在某些特殊后端下，可能需要对输出的 Attribute 进行重新包装或类型调整。
  def getPlanOutput(plan: SparkPlan): Seq[Attribute] = {
    plan.output
  }
  // 创建“数值溢出检查”的 Substrait 表达式节点。
  // 特别针对 Decimal（高精度小数）类型。当进行数值转换或运算时，如果发生溢出，该节点决定是抛出异常还是返回 NULL。不同的后端处理 Decimal 的精度位宽和溢出策略不同，因此需要此接口。
  def createCheckOverflowExprNode(
      context: SubstraitContext,
      substraitExprName: String,
      childNode: ExpressionNode,
      childResultType: DataType,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode
  // 将二进制的 Substrait 计划（JSON 或 Protobuf 字节流）转换为文本字符串。
  def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String
  // 将 Protobuf 消息包装成 google.protobuf.Any 类型。
  // 背景：Substrait 协议经常使用 Any 类型来携带后端自定义的扩展信息。此方法负责将后端特有的配置或元数据序列化。
  def packPBMessage(message: Message): Any

  /** This method is only used for CH backend tests */
  // 清理 SQL 执行资源。
  // 此方法目前专门用于 ClickHouse 后端的测试，用于手动触发特定执行 ID 下的资源释放。
  def invalidateSQLExecutionResource(executionId: String): Unit = {}
  // 作用：为 WriteFilesExecTransformer 生成写入参数。
  def genWriteParameters(write: WriteFilesExecTransformer): Any

  /** use Hadoop Path class to encode the file path */
  // 对文件路径进行编码
  // 默认返回原路径。但有些环境或后端（如处理特殊字符或 Hadoop 通配符时）需要使用 org.apache.hadoop.fs.Path 进行转义编码。
  def encodeFilePathIfNeed(filePath: String): String = filePath
}
