package etl.server.service.staging.impl.job;

import etl.api.dataset.DatasetState;
import etl.api.datasource.CsvDataSource;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.common.hdfs.FileSystemTemplate;
import etl.common.json.MapperWrapper;
import etl.server.domain.entity.ServiceRequest;
import etl.server.domain.entity.StagingDataset;
import etl.server.exception.table.NoSuchTableException;
import etl.server.repository.ServiceRequestRepository;
import etl.server.repository.StagingDatasetRepository;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.StoppableTasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * upload dataset to hdfs by given job instance id
 */
public class DatasetUploadTasklet implements StoppableTasklet {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetUploadTasklet.class);

    @Autowired
    private DatasetConf datasetConf;

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @Autowired
    private StagingDatasetRepository stagingDatasetRepository;

    @Autowired
    private ServiceRequestRepository serviceRequestRepository;

    @Value("#{jobParameters['serviceRequestId']}")
    private long serviceRequestId;

	/**
     * copy file from local to hdfs ,and  move the file to the staged directory once uploaded
     *
     * @param jobExecutionId
     * @param stagingItemRequest
     */
    private Optional<StagingItemRequest> uploadDataset(ChunkContext chunkContext, long jobExecutionId, StagingItemRequest stagingItemRequest) {
        if (!chunkContext.getStepContext().getStepExecution().getJobExecution().getStatus().isRunning()) {
            return Optional.empty();
        }
        beginUpload(jobExecutionId, stagingItemRequest);
        try {
            StagingDataset stagingDataset = upload(jobExecutionId, stagingItemRequest);

            return Optional.of(createStagingDatasetItem(stagingDataset,stagingItemRequest));
        } catch (IOException e) {
            errorUpload(jobExecutionId,stagingItemRequest, e);
        } catch (NoSuchTableException e) {
            LOGGER.error("An error occured while upload dataset", e);
        }
        return Optional.empty();
    }

    private void errorUpload(long jobExecutionId,StagingItemRequest stagingItemRequest, IOException e) {
        CsvDataSource csvSource = (CsvDataSource) stagingItemRequest.getSource();
        LOGGER.warn(String.format("Dataset %s upload failed , the job instance id is %d ", csvSource.getFilePath(), jobExecutionId), e);
        StagingDataset stagingDataset = stagingDatasetRepository.findByJobExecutionIdAndDatasetNameAndMetadataKey(jobExecutionId,
                stagingItemRequest.getDataset().getName(),
                stagingItemRequest.getDataset().toMetadataKey());
        stagingDataset.setStatus(DatasetState.UPLOAD_FAILED);
        stagingDataset.setMessage(e.getMessage());
        stagingDatasetRepository.save(stagingDataset);
    }

    private StagingDataset upload(long jobExecutionId, StagingItemRequest stagingItemRequest) throws IOException {
        StagingDataset stagingDataset = stagingDatasetRepository.findByJobExecutionIdAndDatasetNameAndMetadataKey(jobExecutionId,
                stagingItemRequest.getDataset().getName(),
                stagingItemRequest.getDataset().toMetadataKey());
        CsvDataSource csvSource = (CsvDataSource) stagingItemRequest.getSource();
        //copy file to hdfs
        File localInputFile = new File(StringUtils.appendIfMissing(datasetConf.getLocalPath(), "/") + csvSource.getFilePath());
        fileSystemTemplate.copy(localInputFile, new Path(stagingDataset.getStagingFile()), false);
        stagingDataset.setStatus(DatasetState.UPLOADED);
        return stagingDatasetRepository.save(stagingDataset);
    }

    private void beginUpload(long jobExecutionId, StagingItemRequest stagingItemRequest) {
        StagingDataset stagingDataset = stagingDatasetRepository.findByJobExecutionIdAndDatasetNameAndMetadataKey(jobExecutionId,
                stagingItemRequest.getDataset().getName(),
                stagingItemRequest.getDataset().toMetadataKey());
        stagingDataset.setStatus(DatasetState.UPLOADING);
        stagingDatasetRepository.save(stagingDataset);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ServiceRequest serviceRequest = serviceRequestRepository.findOne(serviceRequestId);
        StagingJobRequest stagingJobRequest = MapperWrapper.MAPPER.readValue( serviceRequest.getRequestMessage(), StagingJobRequest.class);
        //use streaming operator to upload data set
        final List<StagingItemRequest> uploadedStagingItemRequest = stagingJobRequest.getItems()
                .stream()
                .parallel()
                .map(dataset -> this.uploadDataset(chunkContext, serviceRequestId, dataset))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());


        if (uploadedStagingItemRequest.isEmpty()) {
            chunkContext.getStepContext().getStepExecution().setExitStatus(ExitStatus.NOOP);
            chunkContext.getStepContext().getStepExecution().getJobExecution().setStatus(BatchStatus.STOPPED);
        } else {
            StagingJobRequest uploadRequest = StagingJobRequest.builder()
                    .name(stagingJobRequest.getName())
                    .items(uploadedStagingItemRequest)
                    .build();

            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("jobRequest", uploadRequest);
        }

        return RepeatStatus.FINISHED;

    }

    private StagingItemRequest createStagingDatasetItem(StagingDataset stagingDataset,
                                                        StagingItemRequest stagingItemRequest) throws NoSuchTableException {
        return StagingItemRequest.builder()
                .id(stagingDataset.getId())
                .source(stagingItemRequest.getSource())
                .table(stagingItemRequest.getTable())
                .dataset(stagingItemRequest.getDataset())
                .build();

    }

    @Override
    public void stop() {

    }
}