package etl.spark.staging.execution;

import etl.api.dataset.DatasetPath;
import etl.api.job.staging.StagingItemRequest;
import etl.spark.core.DataFrameIOService;
import etl.spark.core.DriverContext;
import etl.spark.staging.config.StagingConfig;
import etl.spark.staging.exception.DatasetStoreException;
import javaslang.control.Either;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;

@Slf4j
@Component
public class DatasetRegisterImpl implements DatasetRegister,Serializable {

    @Autowired
    private transient DriverContext ctx;

    @Autowired
    private  transient StagingListener listener;


    @Autowired
    private transient StagingConfig stagingConfig;

    @Autowired
    private  transient SparkSession sparkSession;

	@Autowired
	private transient DataFrameIOService dataFrameIOService;

    @Override
    public void register(StagingItemRequest item,Either<JavaRDD<String>, Dataset<Row>> validateResult) {
        if (validateResult.isLeft()) {
            log.info("Found invalid records in dataset: {}", item.getDataset().getName());
            listener.onValidationFailed(item.getId());
            storeInvalidDataset(item,validateResult.getLeft());
            log.info("Finished to store invalid records of dataset: {}", item.getDataset().getName());
        } else {
            listener.onValidationFinished(item.getId());
            storeValidDataset(item,validateResult.get());
        }
    }


    private void storeInvalidDataset(StagingItemRequest item,JavaRDD<String> invalidRDD) {

        DatasetPath datasetPath = new DatasetPath(ctx.getJobExecutionId(),
                item.getDataset().getName(),
                stagingConfig.getDatasetRootPath());
        String tmpPath =  datasetPath.getStagingErrorFile() + "_tmp";
        invalidRDD.saveAsTextFile(tmpPath);
        final Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        try (FileSystem fileSystem = FileSystem.get(hadoopConf)) {
            FileUtil.copyMerge(fileSystem,
                    new Path(tmpPath),
                    fileSystem,
                    new Path(datasetPath.getStagingErrorFile()),
                    true,
                    hadoopConf,
                    "");
        } catch (IOException e) {
            throw new DatasetStoreException("An error occurred while store invalid data", e);
        }
    }

    private void storeValidDataset(StagingItemRequest item,Dataset<Row> validDF) {
		try {
			listener.onRegisterStart(item.getId());
            dataFrameIOService.writeDataFrame(validDF, item.getDataset(),item.getTable());
            listener.onRegisterFinished(item.getId());
		} catch (Exception e) {
			listener.onRegisterFailed(item.getId());
			throw new DatasetStoreException("An error occurred while register dataset", e);
		}
    }

}