package etl.spark.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import etl.api.dataset.DatasetMetadata;
import etl.api.dataset.DatasetType;
import etl.client.DatasetClient;
import etl.common.json.MapperWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

public abstract class AbstractDataFrameIOService implements DataFrameIOService {

    @Autowired
    protected DriverContext driverContext;

    @Autowired
    private DatasetClient datasetClient;

    @Override
    public void writeDataFrame(Dataset<Row> dataFrame, DatasetMetadata datasetMetadata,String schemaName) {
        Dataset<Row> createdDataFrame = saveDataFrame(dataFrame, datasetMetadata);
        createDataset(datasetMetadata, createdDataFrame, schemaName);
    }

    private void createDataset(DatasetMetadata datasetMetadata, Dataset<Row> createdDataFrame,String schemaName){
        etl.api.dataset.Dataset dataset = new etl.api.dataset.Dataset();
        dataset.setCreatedTime(new Date());
        dataset.setJobExecutionId(driverContext.getJobExecutionId());
        try {
            dataset.setMetadata(MapperWrapper.MAPPER.writeValueAsString(datasetMetadata));
            dataset.setMetadataKey(datasetMetadata.toMetadataKey());
            dataset.setName(datasetMetadata.getName());
            dataset.setTable(schemaName);
            dataset.setRecordsCount(createdDataFrame.count());
            dataset.setPredicate(buildPredicate(driverContext.getJobExecutionId()));
            dataset.setDatasetType(schemaName != null ?
                    DatasetType.STAGING_DATASET : DatasetType.TRANSFORM_DATASET);
            datasetClient.createDataset(dataset);
        } catch (JsonProcessingException e) {
            throw new DriverException("An error occur while register dataset", e);
        }

    }

    protected abstract String buildPredicate(long runKey);

    protected abstract Dataset<Row> saveDataFrame(Dataset<Row> dataFrame, DatasetMetadata datasetMetadata);
}
