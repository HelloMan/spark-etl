package etl.spark.pipeline.core;

import etl.api.pipeline.DatasetRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Optional;

public interface TransformDatasetService {


    /**
     * persist dataset with specific dataset ref
     * @param stepExecution
     * @param output
     * @param dataset
     */
    void persistDataset(StepExecution stepExecution,DatasetRef output, Dataset<Row> dataset);


    /**
     * get dataset for a specific dataset ref
     * @param stepExecution
     * @param input
     * @return
     */
    Optional<Dataset<Row>> getDataset(StepExecution stepExecution,DatasetRef input);

}
