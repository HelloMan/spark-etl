package etl.spark.core.hive;

import etl.api.dataset.DatasetFieldName;
import etl.api.dataset.DatasetMetadata;
import etl.spark.core.AbstractDataFrameIOService;
import etl.spark.core.DriverContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Using by unit test.
 */
@Slf4j
public abstract class HiveDataFrameIOService extends AbstractDataFrameIOService {
	private static final String ROWKEY_PREDICATE = "%s = %d";

	@Autowired
	private DriverContext ctx;

	@Override
	public Dataset<Row> readDataFrame(etl.api.dataset.Dataset dataset) {
		String sqlPattern = "select * from %s where " + ROWKEY_PREDICATE;
		String sql = String.format(sqlPattern, dataset.getName(), DatasetFieldName.RUN_EXECUTIONN_ID.getName(), dataset.getJobExecutionId());
		return ctx.getSparkSession().sql(sql);
	}


	@Override
	protected Dataset<Row> saveDataFrame(Dataset<org.apache.spark.sql.Row> dataFrame, DatasetMetadata datasetMetadata) {
		boolean tableExists = ctx.getSparkSession().catalog().tableExists(datasetMetadata.getName());
		Dataset<Row> dfWithRunKey = addRunKeyField(dataFrame);
		DataFrameWriter dataFrameWriter = dfWithRunKey.write()
				.partitionBy(DatasetFieldName.RUN_EXECUTIONN_ID.getName());
		if (tableExists) {
			dataFrameWriter.mode(SaveMode.Append);
		}
		dataFrameWriter.saveAsTable(datasetMetadata.getName());
		return dfWithRunKey;
	}



	private Dataset<Row> addRunKeyField(Dataset<Row> dataFrame) {
		Column runKeyColumn = functions.lit(ctx.getJobExecutionId());
		return dataFrame.withColumn(DatasetFieldName.RUN_EXECUTIONN_ID.getName(), runKeyColumn);
	}

	@Override
	public String buildPredicate(long runKey) {
		return String.format(ROWKEY_PREDICATE,
				DatasetFieldName.RUN_EXECUTIONN_ID.getName(), runKey);
	}
}