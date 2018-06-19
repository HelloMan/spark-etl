package etl.server.service.staging.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import etl.api.dataset.DatasetPath;
import etl.api.dataset.DatasetState;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.common.hdfs.FileSystemTemplate;
import etl.common.json.MapperWrapper;
import etl.server.domain.entity.*;
import etl.server.domain.entity.*;
import etl.server.exception.job.JobStartException;
import etl.server.exception.staging.DatasetStateChangeException;
import etl.server.repository.ServiceRequestRepository;
import etl.server.repository.StagingDatasetRepository;
import etl.server.service.common.JobOperator;
import etl.server.service.dataset.DatasetService;
import etl.server.service.staging.StagingDatasetService;
import etl.server.service.staging.impl.job.DatasetConf;
import etl.server.service.table.TableService;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Predicate;
import com.querydsl.jpa.impl.JPAQuery;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

@Service
public class StagingDatasetServiceImpl implements StagingDatasetService {

    @Autowired
    private StagingDatasetRepository stagingDatasetRepository;

    @Autowired
    private ServiceRequestRepository serviceRequestRepository;

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @Autowired
    private DatasetConf datasetConf;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private TableService tableService;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private EntityManager entityManager;

    @Override
    public long start(StagingJobRequest jobRequest) throws JobStartException {
        checkJobRequest(jobRequest);
        return jobOperator.startJob(ServiceRequestType.STAGING.name(), () -> this.createStagingJob(jobRequest), new Properties());
    }

    private void checkJobRequest(StagingJobRequest jobRequest) throws JobStartException {
        for (StagingItemRequest stagingItemRequest : jobRequest.getItems()) {
            tableService.findTable(stagingItemRequest.getTable())
                    .orElseThrow(() -> new JobStartException(String.format("Can't start job due to table not found with table name:%s",
                            stagingItemRequest.getTable())));

        }
//
        JPAQuery<StagingDataset> query = new JPAQuery<>(entityManager);
        QueryResults<com.querydsl.core.Tuple> tupleQueryResults = query.select(QStagingDataset.stagingDataset.datasetName, QServiceRequest.serviceRequest).from(QStagingDataset.stagingDataset)
                .join(QServiceRequest.serviceRequest)
                .on(QServiceRequest.serviceRequest.id.eq(QStagingDataset.stagingDataset.jobExecutionId))
                .limit(10).offset(20)
                .orderBy()
                .fetchResults();

        Predicate booleanExpression = QStagingDataset.stagingDataset.datasetName.eq("s");

        stagingDatasetRepository.findAll(booleanExpression);
    }

    protected ServiceRequest createStagingJob(StagingJobRequest jobRequest) {
        ServiceRequest serviceRequest = new ServiceRequest();
        serviceRequest.setName(jobRequest.getName());
        serviceRequest.setServiceRequestType(ServiceRequestType.STAGING);

        try {
            serviceRequest.setRequestMessage(MapperWrapper.MAPPER.writeValueAsString(jobRequest));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
        final long jobExecutionId = serviceRequestRepository.save(serviceRequest).getId();

        jobRequest.getItems().forEach(itemRequest -> this.createStagingDataset(itemRequest, jobExecutionId));

        return serviceRequest;
    }

    private void createStagingDataset(StagingItemRequest itemRequest, long jobExecutionId) {
        StagingDataset dataset = new StagingDataset();
        dataset.setStartTime(new Date());
        dataset.setTable(itemRequest.getTable());
        dataset.setJobExecutionId(jobExecutionId);
        dataset.setLastUpdateTime(new Date());
        DatasetPath datasetPath = new DatasetPath(jobExecutionId,
                itemRequest.getDataset().getName(),
                datasetConf.getRemotePath());
        dataset.setDatasetName(itemRequest.getDataset().getName());
        try {
            dataset.setMetadataKey(itemRequest.getDataset().toMetadataKey());
            dataset.setMetadata(MapperWrapper.MAPPER.writeValueAsString(itemRequest.getDataset()));
            dataset.setStagingFile(datasetPath.getStagingFile());
            dataset.setValidationErrorFile(datasetPath.getStagingErrorFile());
            stagingDatasetRepository.save(dataset);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("An error occurred while ",e);
        }


    }


    @Override
    public Optional<StagingDataset> findStagingDataset(long stagingDatasetId) {
        return Optional.ofNullable(stagingDatasetRepository.findOne(stagingDatasetId));
    }

    @Override
    public List<StagingDataset> findStagingDatasets(long jobExecutionId, String dataset) {
        if (StringUtils.isNotEmpty(dataset)) {
            return stagingDatasetRepository.findByJobExecutionIdAndDatasetName(jobExecutionId, dataset);
        }
        return stagingDatasetRepository.findByJobExecutionId(jobExecutionId);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Tuple2<String, InputStream>> getStagingDatasetError(long stagingDatasetId) throws IOException {
        Optional<StagingDataset> dataset = findStagingDataset(stagingDatasetId);
        if (dataset.isPresent()) {
            String fileName = dataset.get().getValidationErrorFile();
            try(InputStream inputStream = fileSystemTemplate.open(new Path(fileName))) {
                return Optional.of(Tuple.of(new Path(fileName).getName(), inputStream));
            }
        } else {
            return Optional.empty();
        }

    }

    @Override
    public void updateStagingDatasetState(long stagingDatasetId, String state) throws
            DatasetStateChangeException {
        DatasetState dataSetState = DatasetState.lookupState(state).orElseThrow(() -> new DatasetStateChangeException(String.format("dataset state %s can not be found", state)));
        this.updateDatasetState(stagingDatasetId, dataSetState);

    }


    private void updateDatasetState(long stagingDatasetId, DatasetState state) throws DatasetStateChangeException {
        Objects.requireNonNull(state, "state is empty");
        Optional<StagingDataset> stagingDatasetOptional = findStagingDataset(stagingDatasetId);
        if (!stagingDatasetOptional.isPresent()) {
            throw new DatasetStateChangeException(String.format("staging dataset can't be found with stagingDatasetId %d", stagingDatasetId));
        }
        StagingDataset stagingDataset = stagingDatasetOptional.get();
        if (!stagingDataset.getStatus().canTransistTo(state)) {
            throw new DatasetStateChangeException(String.format("Can't transit dataset state from %s to %s with stagingDatasetId:%d",
                    stagingDataset.getStatus(), state, stagingDatasetId));
        }
        if (state == DatasetState.VALIDATION_FAILED ||
                state == DatasetState.REGISTRATION_FAILED ||
                state == DatasetState.REGISTERED) {
            stagingDataset.setEndTime(new Date());
        }
        stagingDataset.setLastUpdateTime(new Date());
        stagingDataset.setStatus(state);
        stagingDatasetRepository.save(stagingDataset);
    }


}