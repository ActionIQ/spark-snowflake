package net.snowflake.spark.snowflake.io

import java.io.InputStream
import net.snowflake.spark.snowflake.SparkConnectorContext
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.DataSourceTelemetryNamespace.DATASOURCE_TELEMETRY_METRICS_NAMESPACE
import org.apache.spark.{DataSourceTelemetryHelpers, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

class SnowflakeRDD(sc: SparkContext,
                   fileNames: List[String],
                   format: SupportedFormat,
                   downloadFile: String => InputStream,
                   expectedPartitionCount: Int)
    extends RDD[String](sc, Nil) with DataSourceTelemetryHelpers {

  @transient private val MIN_FILES_PER_PARTITION = 2
  @transient private val MAX_FILES_PER_PARTITION = 10

  override def compute(split: Partition,
                       context: TaskContext): Iterator[String] = {
    // Log system configuration on executor
    SparkConnectorContext.recordConfig()

    val snowflakePartition = split.asInstanceOf[SnowflakePartition]

    val stringIterator = new SFRecordReader(format, snowflakePartition.index)
    stringIterator.setDownloadFunction(downloadFile)

    snowflakePartition.fileNames.foreach(name => {
      stringIterator.addFileName(name)
    })

    logger.info(
      logEventNameTagger(
        // scalastyle:off line.size.limit
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: Start reading
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.partitionId=${snowflakePartition.index}
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.totalFileCount=${snowflakePartition.fileNames.size}
           |""".stripMargin.linesIterator.mkString(" ")
        // scalastyle:on line.size.limit
      )
    )

    stringIterator
  }

  override protected def getPartitions: Array[Partition] = {
    var fileCountPerPartition =
      Math.max(
        MIN_FILES_PER_PARTITION,
        (fileNames.length + expectedPartitionCount / 2) / expectedPartitionCount
      )
    fileCountPerPartition = Math.min(MAX_FILES_PER_PARTITION, fileCountPerPartition)
    val fileCount = fileNames.length
    val partitionCount = (fileCount + fileCountPerPartition - 1) / fileCountPerPartition
    logger.info(
      logEventNameTagger(
        s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}: Total statistics:
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.fileCount=$fileCount
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.filePerPartition=$fileCountPerPartition
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.actualPartitionCount=$partitionCount
           |$DATASOURCE_TELEMETRY_METRICS_NAMESPACE.expectedPartitionCount=$expectedPartitionCount
           |""".stripMargin.linesIterator.mkString(" ")
      )
    )

    if (fileNames.nonEmpty) {
      fileNames
        .grouped(fileCountPerPartition)
        .zipWithIndex
        .map {
          case (names, index) => SnowflakePartition(names, id, index)
        }
        .toArray
    } else {
      // If the result set is empty, put one empty partition to the array.
      Seq[SnowflakePartition]{SnowflakePartition(fileNames, 0, 0)}.toArray
    }
  }

}

private case class SnowflakePartition(fileNames: List[String],
                                      rddId: Int,
                                      index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}
