package etl.server.service.dataset;

import etl.api.dataset.Dataset;
import etl.api.dataset.DatasetMetadata;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface DatasetService {

    Dataset createDataset(Dataset dataset);

    Optional<Dataset> findLastDataset(DatasetMetadata datasetMetadata);

    List<Dataset> findDatasets(DatasetMetadata datasetMetadata);

    List<Dataset> findDatasets(String dataset);

    Optional<Dataset> findDataset(long jobExecutionId, DatasetMetadata datasetMetadata);

    List<Dataset> findDatasets(long jobExecutionId);

    Set<String> getAllDatasets();

}
