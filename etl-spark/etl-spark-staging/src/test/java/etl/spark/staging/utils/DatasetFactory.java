package etl.spark.staging.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import etl.api.dataset.Dataset;
import etl.api.dataset.DatasetType;
import etl.api.parameter.Parameters;
import etl.common.json.MapperWrapper;

import java.util.Date;

public class DatasetFactory {

    public static Dataset createDataset(String datasetName,Parameters metadata){
        Dataset dataset = new Dataset();
        dataset.setCreatedTime(new Date());
        dataset.setDatasetType(DatasetType.STAGING_DATASET);
        dataset.setId(1l);
        dataset.setJobExecutionId(1l);
        try {
            dataset.setMetadata(MapperWrapper.MAPPER.writeValueAsString(metadata));
            dataset.setName(datasetName);
            dataset.setTable(datasetName);
            return dataset;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }


    }
}
