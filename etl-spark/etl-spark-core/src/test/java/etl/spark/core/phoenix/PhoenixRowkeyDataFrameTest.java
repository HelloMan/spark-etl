package etl.spark.core.phoenix;

import com.google.common.collect.ImmutableList;
import etl.api.dataset.DatasetFieldName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PhoenixRowkeyDataFrameTest {

	private SparkSession spark;

	@Before
	public void init() {
		spark = SparkSession.builder().appName("DataFrameHelperTest").master("local[*]").getOrCreate();
	}

	@Test
	public void addRowKeyField_dataFrame_without_rowKey() throws Exception {
		StructField f1 = new StructField("id", DataTypes.IntegerType, true, Metadata.empty());
		StructField[] fields = {f1};
		StructType structType = new StructType(fields);

		Row row1 = RowFactory.create(1);
		Dataset<Row> dataFrame = spark.createDataFrame(ImmutableList.of(row1), structType);

		PhoenixRowkeyDataFrame helper = new  PhoenixRowkeyDataFrame(spark, dataFrame);
		Dataset<Row> newDF = helper.withRowkey(1L);

		assertThat(newDF.schema().fieldIndex(DatasetFieldName.ROW_KEY.getName())).isEqualTo(0);
	}

	@Test
	public void addRowKeyField_dataFrame_with_rowKey() throws Exception {
		StructField f1 = new StructField(DatasetFieldName.ROW_KEY.getName(), DataTypes.LongType, true, Metadata.empty());
		StructField f2 = new StructField("id", DataTypes.IntegerType, true, Metadata.empty());
		StructField[] fields = {f1, f2};
		StructType structType = new StructType(fields);

		Long rowSeq = 0L;
		Long newRunKey = 10L;

		PhoenixTableRowKey rowKeyHelper = PhoenixTableRowKey.of(1L);
		Row row1 = RowFactory.create(rowKeyHelper.buildRowKey(rowSeq), 123);
		Dataset<Row> dataFrame = spark.createDataFrame(ImmutableList.of(row1), structType);

		PhoenixRowkeyDataFrame helper = new  PhoenixRowkeyDataFrame(spark, dataFrame);
		Dataset<Row> newDF = helper.withRowkey(newRunKey);

		assertThat(newDF.schema().fieldIndex(DatasetFieldName.ROW_KEY.getName())).isEqualTo(0);

		long rowKey = newDF.collectAsList().get(0).getLong(0);
		PhoenixTableRowKey rowKeyHelper2 = PhoenixTableRowKey.of(newRunKey);
		assertThat(rowKey).isEqualTo(rowKeyHelper2.buildRowKey(rowSeq));
	}
}