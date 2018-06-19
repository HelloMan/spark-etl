package etl.spark.core;

import etl.api.dataset.DatasetMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataFrameIOService {

	void writeDataFrame(Dataset<Row> dataFrame,DatasetMetadata datasetMetadata,String schemaName);

	Dataset<Row> readDataFrame(etl.api.dataset.Dataset dataset);


}