package etl.spark.pipeline.core;

import etl.api.pipeline.DatasetRef;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobExecutionContext {

    private final Map<DatasetRef, Dataset<Row>> cacheDataset;

    @Getter
    private final JobExecution jobExecution;

    public JobExecutionContext(JobExecution jobExecution) {
        this.jobExecution = jobExecution;
        cacheDataset = new ConcurrentHashMap<>();
    }


    public void addDataset(DatasetRef datasetRef,Dataset<Row> dataset) {
        cacheDataset.put(datasetRef, dataset);
    }
    public Dataset<Row> getDataset(DatasetRef datasetRef) {
        return cacheDataset.get(datasetRef);
    }
}
