package etl.server.service.dataset;

import etl.api.dataset.Dataset;
import etl.api.dataset.DatasetMetadata;
import etl.server.repository.DatasetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Service
@Transactional
public class DatasetServiceImpl implements DatasetService {

    @Autowired
    private DatasetRepository datasetRepository;

    @Override
    public Dataset createDataset(Dataset dataset) {
        return datasetRepository.save(dataset);
    }

    @Override
    public Optional<Dataset> findLastDataset(DatasetMetadata datasetMetadata) {
        Objects.requireNonNull(datasetMetadata);
        return Optional.ofNullable(datasetRepository.findLastDatasetOf(datasetMetadata.getName(), datasetMetadata.toMetadataKey()));
    }

	@Override
    public List<Dataset> findDatasets(DatasetMetadata datasetMetadata) {
        Objects.requireNonNull(datasetMetadata);
        return datasetRepository.findByNameAndMetadataKey(datasetMetadata.getName(), datasetMetadata.toMetadataKey());
    }

    @Override
    public List<Dataset> findDatasets(String dataset) {
        return datasetRepository.findByName(dataset);
    }


    @Override
    public Optional<Dataset> findDataset(long jobExecutionId, DatasetMetadata datasetMetadata) {
        Objects.requireNonNull(datasetMetadata);
        return Optional.ofNullable(datasetRepository.findDataset(jobExecutionId, datasetMetadata.getName(), datasetMetadata.toMetadataKey()));
    }

    @Override
    public List<Dataset> findDatasets(long jobExecutionId) {
        return datasetRepository.findByJobExecutionId(jobExecutionId);
    }

    @Override
    public Set<String> getAllDatasets() {
        return datasetRepository.findAllDatasets();
    }
}
