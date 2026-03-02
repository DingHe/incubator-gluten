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

import org.apache.gluten.config.{HashShuffleWriterType, ShuffleWriterType}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleReaderParameters, GenShuffleWriterParameters, GlutenShuffleReaderWrapper, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.types.{DecimalType, LongType, NullType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

// SparkPlanExecApi 是 Apache Gluten 架构中的物理执行层转换接口。
// 它定义了如何将原生的 SparkPlan（物理算子）和 Expression（表达式）映射并转换为 Gluten 的 Transformer 算子。
// 简单来说，当 Gluten 决定将某个查询下推到 Native 引擎时，它会调用这个 API 来创建对应的“翻译算子”。
// 算子翻译工厂：它是生成各类 ExecTransformer（如 FilterExecTransformer）的工厂类。
// 屏蔽后端差异：不同的计算后端（Velox, ClickHouse）对算子的实现细节不同（例如 Join 的实现、聚合的逻辑）。SparkPlanExecApi 允许后端注入自己特有的转换逻辑。
// 连接 Spark 与 Substrait：它负责提取 Spark 算子中的元数据（如 Key、Condition、Schema），并为后续生成 Substrait 计划做好准备。
// Shuffle 与 Exchange 管理：定义了列式 Shuffle（Columnar Shuffle）的依赖生成、序列化及读写器的创建。
trait SparkPlanExecApi {

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  // 生成过滤（Filter）和投影（Project）的转换器。这是 SQL 执行中最基础的两个算子。
  def genFilterExecTransformer(condition: Expression, child: SparkPlan): FilterExecTransformerBase

  def genProjectExecTransformer(
      projectList: Seq[NamedExpression],
      child: SparkPlan): ProjectExecTransformer =
    ProjectExecTransformer.createUnsafe(projectList, child)

  /** Generate HashAggregateExecTransformer. */
  // 生成基于哈希的聚合算子。它处理分组表达式（Grouping）和聚合函数（Aggregate Expressions）。
  def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer

  /** Generate HashAggregateExecPullOutHelper */
  // 辅助方法。由于 Native 聚合后的属性可能需要重新映射回 Spark 识别的属性，该 Helper 负责这种属性的“拉出”逻辑。
  def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper
  // 将 Spark 的 ShuffleExchangeExec 替换为 Gluten 的列式版本。
  def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan

  /** Generate ShuffledHashJoinExecTransformer. */
  // 生成混合哈希连接（SHJ）转换器。
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase

  /** Generate BroadcastHashJoinExecTransformer. */
  // 生成广播哈希连接（BHJ）转换器。
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase

  def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer

  /** Generate ShuffledHashJoinExecTransformer. */
  // 生成排序归并连接（SMJ）转换器。
  def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase

  /** Generate CartesianProductExecTransformer. */
  def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer

  def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer
  // 处理别名（Alias）。
  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    AliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  // 处理复杂类型（数组、映射）的取值。
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer

  def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genFromJsonTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: JsonToStructs): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genToJsonTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StructsToJson): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  def genUnbase64Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: UnBase64): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  def genBase64StaticInvokeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StaticInvoke): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  /** Transform GetArrayItem to Substrait. */
  // 处理复杂类型（数组、映射）的取值。
  def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer

  /** Transform NaNvl to Substrait. */
  def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  def genAtLeastNNonNullsTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: AtLeastNNonNulls): ExpressionTransformer = {
    throw new GlutenNotSupportException("AtLeastNNonNulls is not supported")
  }

  def genUuidTransformer(substraitExprName: String, original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(), original)
  }

  def genTryArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: TryEval,
      checkArithmeticExprName: String): ExpressionTransformer = {
    throw new GlutenNotSupportException(s"$checkArithmeticExprName is not supported")
  }

  def genTryEvalTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: TryEval): ExpressionTransformer = {
    throw new GlutenNotSupportException(s"try_eval(${original.child.prettyName}) is not supported")
  }
  // 处理算术运算。
  def genArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression,
      checkArithmeticExprName: String): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  def getDecimalArithmeticExprName(exprName: String): String = exprName

  /** Transform map_entries to Substrait. */
  def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("map_entries is not supported")
  }

  /** Transform array filter to Substrait. */
  def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    throw new GlutenNotSupportException("filter(on array) is not supported")
  }

  /** Transform array forall to Substrait. */
  def genArrayForAllTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayForAll): ExpressionTransformer = {
    throw new GlutenNotSupportException("all_match is not supported")
  }

  /** Transform array array_sort to Substrait. */
  def genArraySortTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArraySort): ExpressionTransformer = {
    throw new GlutenNotSupportException("array_sort(on array) is not supported")
  }

  /** Transform array exists to Substrait */
  def genArrayExistsTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayExists): ExpressionTransformer = {
    throw new GlutenNotSupportException("any_match is not supported")
  }

  /** Transform array transform to Substrait. */
  def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    throw new GlutenNotSupportException("transform(on array) is not supported")
  }

  /** Transform inline to Substrait. */
  def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("inline is not supported")
  }

  /** Transform posexplode to Substrait. */
  def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer

  /** Transform make_timestamp to Substrait. */
  def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("make_timestamp is not supported")
  }

  def genRegexpReplaceTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: RegExpReplace): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genPreciseTimestampConversionTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: PreciseTimestampConversion): ExpressionTransformer = {
    throw new GlutenNotSupportException("PreciseTimestampConversion is not supported")
  }

  def genArrayInsertTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("ArrayInsert is not supported")
  }

  // For date_add(cast('2001-01-01' as Date), interval 1 day), backends may handle it in different
  // ways
  // 处理日期相关的计算，这些在不同后端（如 Velox vs CH）中通常有完全不同的函数签名。
  def genDateAddTransformer(
      attributeSeq: Seq[Attribute],
      substraitExprName: String,
      children: Seq[Expression],
      expr: Expression): ExpressionTransformer = {
    val childrenTransformers =
      children.map(ExpressionConverter.replaceWithExpressionTransformer(_, attributeSeq))
    GenericExpressionTransformer(substraitExprName, childrenTransformers, expr)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * childOutputAttributes may be different from outputAttributes, for example, the
   * childOutputAttributes include additional shuffle key columns
   *
   * @return
   */
  // scalastyle:off argcount
  // 生成 ShuffleDependency。这是 Spark 调度系统识别 Shuffle 的核心对象，Gluten 在这里注入了列式序列化器。
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]

  /** Determine whether to use sort-based shuffle based on shuffle partitioning and output. */
  def getShuffleWriterType(
      partitioning: Partitioning,
      output: Seq[Attribute]): ShuffleWriterType = {
    HashShuffleWriterType
  }

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  // 创建 Native 侧优化的 Shuffle 写入器（支持压缩、分区等）和读取器。
  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V]
  // 创建 Native 侧优化的 Shuffle 写入器（支持压缩、分区等）和读取器。
  def genColumnarShuffleReader[K, C](
      parameters: GenShuffleReaderParameters[K, C]): GlutenShuffleReaderWrapper[K, C]

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // 创建用于传输 ColumnarBatch 的专用序列化器。
  def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType): Serializer

  /** Create broadcast relation for BroadcastExchangeExec */
  def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation

  def doCanonicalizeForBroadcastMode(mode: BroadcastMode): BroadcastMode = {
    mode.canonicalized
  }

  /** Create ColumnarWriteFilesExec */
  // 创建列式写文件算子，用于 Native 存储写入。
  def createColumnarWriteFilesExec(
      child: WriteFilesExecTransformer,
      noop: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec

  /** Create ColumnarArrowEvalPythonExec, for velox backend */
  // 为支持 Python UDF 的后端（如 Velox）创建基于 Arrow 的 Python 执行算子。
  def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan

  def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    GetStructFieldTransformer(substraitExprName, childTransformer, original)
  }

  def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, original)
  }

  def genStringTranslateTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      matchingExpr: ExpressionTransformer,
      replaceExpr: ExpressionTransformer,
      original: StringTranslate): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(srcExpr, matchingExpr, replaceExpr),
      original)
  }

  def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer

  /**
   * Generate an ExpressionTransformer to transform TruncTimestamp expression.
   * TruncTimestampTransformer is the default implementation.
   */
  def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String] = None,
      original: TruncTimestamp): ExpressionTransformer = {
    TruncTimestampTransformer(substraitExprName, format, timestamp, original)
  }

  def genToUnixTimestampTransformer(
      substraitExprName: String,
      timeExp: ExpressionTransformer,
      format: ExpressionTransformer,
      original: Expression): ExpressionTransformer
  // 处理日期相关的计算，这些在不同后端（如 Velox vs CH）中通常有完全不同的函数签名。
  def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer

  def genCastWithNewChild(c: Cast): Cast = c

  def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, exprs, original)
  }

  /** Define backend-specific expression mappings. */
  // 允许后端定义自己特有的表达式映射（例如某个后端支持特定的自定义函数）。
  def extraExpressionMappings: Seq[Sig] = Seq.empty

  /** Define backend-specific expression converter. */
  // 允许后端定义自己特有的表达式映射（例如某个后端支持特定的自定义函数）。
  def extraExpressionConverter(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): Option[ExpressionTransformer] =
    None

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  // 一个布尔检查，用于判断某种 Join 类型或条件是否不被后端支持，从而决定是否回退（Fallback）到 Spark 原生执行。
  def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      right: AttributeSet,
      condition: Option[Expression]): Boolean = false

  /** default function to generate window function node */
  // 这是代码中最长的一个默认实现方法。它解析 Spark 的 WindowExpression（如 RowNumber, Rank, Lead, Lag 等），并根据窗口帧（Frame）定义，手动构造 Substrait 协议中的窗口函数节点。
  def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: JList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      context: SubstraitContext): Unit = {
    windowExpression.map {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = s"${aliasExpr.name}_${aliasExpr.exprId.id}"
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
            val aggWindowFunc = wf.asInstanceOf[AggregateWindowFunction]
            val frame = aggWindowFunc.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, aggWindowFunc).toInt,
              new JArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(aggWindowFunc.dataType, aggWindowFunc.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val aggregateFunc = aggExpression.aggregateFunction
            val substraitAggFuncName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
            if (substraitAggFuncName.isEmpty) {
              throw new GlutenNotSupportException(s"Not currently supported: $aggregateFunc.")
            }

            val childrenNodeList = aggregateFunc.children
              .map(
                ExpressionConverter
                  .replaceWithExpressionTransformer(_, originalInputAttributes)
                  .doTransform(context))
              .asJava

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(context, aggExpression.aggregateFunction).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ (_: Lead | _: Lag) =>
            val offsetWf = wf.asInstanceOf[FrameLessOffsetWindowFunction]
            val frame = offsetWf.frame.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(context))
            // Spark only accepts foldable offset. Converts it to LongType literal.
            val offset = offsetWf.offset.eval(EmptyRow).asInstanceOf[Int]
            // Velox only allows negative offset. WindowFunctionsBuilder#create converts
            // lag/lead with negative offset to the function with positive offset. So just
            // makes offsetNode store positive value.
            val offsetNode = ExpressionBuilder.makeLiteral(Math.abs(offset.toLong), LongType, false)
            childrenNodeList.add(offsetNode)
            // NullType means Null is the default value. Don't pass it to native.
            if (offsetWf.default.dataType != NullType) {
              childrenNodeList.add(
                ExpressionConverter
                  .replaceWithExpressionTransformer(
                    offsetWf.default,
                    attributeSeq = originalInputAttributes)
                  .doTransform(context))
            }
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, offsetWf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(offsetWf.dataType, offsetWf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              offsetWf.ignoreNulls,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NthValue(input, offset: Literal, ignoreNulls: Boolean) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(input, attributeSeq = originalInputAttributes)
                .doTransform(context))
            childrenNodeList.add(LiteralTransformer(offset).doTransform(context))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              ignoreNulls,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NTile(buckets: Expression) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            val literal = buckets.asInstanceOf[Literal]
            childrenNodeList.add(LiteralTransformer(literal).doTransform(context))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case _ =>
            throw new GlutenNotSupportException(
              "unsupported window function type: " +
                wExpression.windowFunction)
        }
    }
  }

  def rewriteSpillPath(path: String): String = path

  def supportPushDownFilterToScan(sparkExecNode: LeafExecNode): Boolean = true

  /** Return whether the filter is supported in scan. */
 // 校验某个过滤条件是否能被下推到文件扫描层（Scan）
  def isSupportedScanFilter(filter: Expression, sparkExecNode: LeafExecNode): Boolean = {
    ExpressionConverter.canReplaceWithExpressionTransformer(
      ExpressionConverter.replaceAttributeReference(filter),
      sparkExecNode.output) &&
    (!filter.references.exists(
      attr => BackendsApiManager.getSparkPlanExecApiInstance.isRowIndexMetadataColumn(attr.name)))
  }

  def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan
  ): GenerateExecTransformerBase

  def genPreProjectForGenerate(generate: GenerateExec): SparkPlan

  def genPostProjectForGenerate(generate: GenerateExec): SparkPlan

  def genPreProjectForArrowEvalPythonExec(arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan =
    arrowEvalPythonExec

  def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = plan
  // 精确计算 Decimal 类型在 Round 运算后的精度（Precision）和标度（Scale），防止溢出。
  def genDecimalRoundExpressionOutput(decimalType: DecimalType, toScale: Int): DecimalType = {
    val p = decimalType.precision
    val s = decimalType.scale
    // After rounding we may need one more digit in the integral part,
    // e.g. `ceil(9.9, 0)` -> `10`, `ceil(99, -1)` -> `100`.
    val integralLeastNumDigits = p - s + 1
    if (toScale < 0) {
      // negative scale means we need to adjust `-scale` number of digits before the decimal
      // point, which means we need at lease `-scale + 1` digits (after rounding).
      val newPrecision = math.max(integralLeastNumDigits, -toScale + 1)
      // We have to accept the risk of overflow as we can't exceed the max precision.
      DecimalType(math.min(newPrecision, DecimalType.MAX_PRECISION), 0)
    } else {
      val newScale = math.min(s, toScale)
      // We have to accept the risk of overflow as we can't exceed the max precision.
      DecimalType(math.min(integralLeastNumDigits + newScale, 38), newScale)
    }
  }

  def genWindowGroupLimitTransformer(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      rankLikeFunction: Expression,
      limit: Int,
      mode: GlutenWindowGroupLimitMode,
      child: SparkPlan): SparkPlan =
    WindowGroupLimitExecTransformer(partitionSpec, orderSpec, rankLikeFunction, limit, mode, child)

  def genHiveUDFTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    HiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
  }

  def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): ExpressionTransformer =
    GenericExpressionTransformer(substraitExprName, Seq(srcExpr, regexExpr, limitExpr), original)

  def genColumnarCollectLimitExec(
      limit: Int,
      plan: SparkPlan,
      offset: Int): ColumnarCollectLimitBaseExec
  // 转换 Range 算子（生成数字序列）。
  def genColumnarRangeExec(rangeExec: RangeExec): ColumnarRangeBaseExec

  def genColumnarTailExec(limit: Int, plan: SparkPlan): ColumnarCollectTailBaseExec

  def genColumnarToCarrierRow(plan: SparkPlan): SparkPlan

  def expressionFlattenSupported(expr: Expression): Boolean = false

  def genFlattenedExpressionTransformer(
      substraitName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer =
    GenericExpressionTransformer(substraitName, children, expr)

  def isSupportRDDScanExec(plan: RDDScanExec): Boolean = false

  def getRDDScanTransform(plan: RDDScanExec): RDDScanTransformer =
    throw new GlutenNotSupportException("RDDScanExec is not supported")

  def copyColumnarBatch(batch: ColumnarBatch): ColumnarBatch =
    throw new GlutenNotSupportException("Copying ColumnarBatch is not supported")

  def serializeColumnarBatch(output: ObjectOutputStream, batch: ColumnarBatch): Unit =
    throw new GlutenNotSupportException("Serialize ColumnarBatch is not supported")

  def deserializeColumnarBatch(input: ObjectInputStream): ColumnarBatch =
    throw new GlutenNotSupportException("Deserialize ColumnarBatch is not supported")

  def genTimestampAddTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer

  def genTimestampDiffTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("timestampdiff is not supported")
  }

  def genMonthsBetweenTransformer(
      substraitExprName: String,
      date1: ExpressionTransformer,
      date2: ExpressionTransformer,
      roundOff: ExpressionTransformer,
      original: MonthsBetween): ExpressionTransformer

  def isRowIndexMetadataColumn(columnName: String): Boolean = {
    SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(columnName)
  }

  def getErrorMessage(raiseError: RaiseError): Expression = {
    throw new GlutenNotSupportException(s"${ExpressionNames.RAISE_ERROR} is not supported")
  }
}
