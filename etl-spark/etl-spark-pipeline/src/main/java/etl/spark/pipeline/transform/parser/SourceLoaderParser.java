package etl.spark.pipeline.transform.parser;

import com.google.common.collect.ImmutableMap;
import etl.api.datasource.CsvDataSource;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.SourceLoader;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.spark.sql.SparkSession;

/**
 * Just for running local example, won't be used in product.
 */
public class SourceLoaderParser extends AbstractTransformParser<SourceLoader> {

	private final SparkSession sparkSession;


	public SourceLoaderParser(TransformDatasetService transformDatasetService,SparkSession sparkSession,StepExecution stepExecution) {
		super(transformDatasetService,stepExecution);
		this.sparkSession = sparkSession;
	}

	@Override
	public void parse(SourceLoader transform ) {
		Class srcClass = transform.getSource().getClass();
		if (CsvDataSource.class.isAssignableFrom(srcClass)) {

			loadCsvSource(transform);
		} else {
			throw new IllegalArgumentException("Unable to parse source type: " + srcClass);
		}
	}
	
	private void loadCsvSource(SourceLoader sourceLoader) {
		CsvDataSource csvSource = (CsvDataSource) sourceLoader.getSource();
		ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
		options.put("inferSchema", "true");
		if (csvSource.isHeader()) {
			options.put("header", "true");
		}
		org.apache.spark.sql.Dataset src = sparkSession
				.read()
				.options(options.build())
				.csv(csvSource.getFilePath());
		transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(sourceLoader), src);
	}
}