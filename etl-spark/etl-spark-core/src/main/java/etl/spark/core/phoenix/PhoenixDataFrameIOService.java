package etl.spark.core.phoenix;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import etl.api.dataset.DatasetFieldName;
import etl.api.dataset.DatasetMetadata;
import etl.api.job.JobConfig;
import etl.spark.core.AbstractDataFrameIOService;
import etl.spark.core.DataBaseSchema;
import etl.spark.core.DriverContext;
import etl.spark.core.DriverException;
import etl.spark.util.ScalaOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.phoenix.spark.DataFrameFunctions;
import org.apache.phoenix.spark.SparkSqlContextFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Predef;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.sql.SQLException;
import java.util.Map;

@Service
@Slf4j
public class PhoenixDataFrameIOService extends AbstractDataFrameIOService {

	private static final String ROWKEY_PREDICATE = "%s >= %d and %s <= %d";

	@Autowired
	private DriverContext ctx;

	@Override
	protected Dataset<Row> saveDataFrame(Dataset<Row> dataFrame, DatasetMetadata datasetMetadata) {
		Dataset<Row> dfWithRowKey = new PhoenixRowkeyDataFrame(ctx.getSparkSession(), dataFrame)
				.withRowkey(ctx.getJobExecutionId());
		createTable(dfWithRowKey, datasetMetadata.getName());

		Map<String, String> map = ImmutableMap.of(
				"table", datasetMetadata.getName().toUpperCase(),
				"zkUrl", ctx.getProperty(JobConfig.ZOOKEEPER_URL)
		);
		DataFrameFunctions dff = new DataFrameFunctions(dfWithRowKey);
		scala.collection.immutable.Map<String, String> scalaMap =
				JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(Predef.<Tuple2<String, String>>conforms());
		dff.saveToPhoenix(scalaMap);

		return dfWithRowKey;

	}

	private void createTable(Dataset<Row> dataFrame, String tableName) {
		DataBaseSchema dataBaseSchema = new PhoenixSchema(
				ctx.getProperty(JobConfig.ZOOKEEPER_URL),
				ctx.getProperty(JobConfig.HBASE_ROOTDIR),
				ctx.getInteger(JobConfig.PHOENIX_SALTBUCKETS));

		try {
			if (!dataBaseSchema.tableExists(tableName)) {
                dataBaseSchema.createTable(tableName,dataFrame.schema());
            }
		} catch (SQLException e) {
			throw new DriverException(e);
		}
	}

	@Override
	public Dataset<Row> readDataFrame(etl.api.dataset.Dataset dataset) {
		log.info("load phoenix table: {} with run key: {}", dataset.getName(), dataset.getJobExecutionId());
		SparkSqlContextFunctions sqlContextFunctions = new SparkSqlContextFunctions(ctx.getSparkSession().sqlContext());
		return sqlContextFunctions.phoenixTableAsDataFrame(
				dataset.getName(),
				JavaConversions.asScalaBuffer(Lists.<String>newArrayList()).toSeq(),
				new Some(dataset.getPredicate()),
				new Some(ctx.getProperty(JobConfig.ZOOKEEPER_URL)),
				ScalaOption.NONE,
				ctx.getSparkSession().sparkContext().hadoopConfiguration());
	}

	@Override
	protected String buildPredicate(long runKey) {
		javaslang.Tuple2<Long, Long> range = PhoenixTableRowKey.of(runKey).getRowKeyRange();
		return String.format(ROWKEY_PREDICATE,
				DatasetFieldName.ROW_KEY.getName(), range._1,
				DatasetFieldName.ROW_KEY.getName(), range._2);
	}



}