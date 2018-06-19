package etl.server.service.staging.impl.job;

import etl.api.dataset.DatasetMetadata;
;

import etl.api.datasource.DataSources;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.common.hdfs.FileSystemTemplate;
import etl.common.json.MapperWrapper;
import etl.server.domain.entity.ServiceRequest;
import etl.server.domain.entity.StagingDataset;
import etl.server.repository.ServiceRequestRepository;
import etl.server.repository.StagingDatasetRepository;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

import java.io.File;
import java.io.FileOutputStream;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatasetUploadTaskletTest {
	@Mock
	private DatasetConf datasetConf;

	@Mock
	private FileSystemTemplate fileSystemTemplate;

	@Mock
	private StagingDatasetRepository stagingDatasetRepository;

	@Mock
	private StepContribution stepContribution;

	@Mock
	private ServiceRequestRepository serviceRequestRepository;

	@Mock
	ChunkContext chunkContext;

	@InjectMocks
	DatasetUploadTasklet datasetUploadTasklet;

	private long jobExecutionId = 1L;

	@Before
	public void setup() throws Exception {
		String csvPath = new File(getClass().getClassLoader().getResource("datasets").getFile()).getAbsolutePath();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("datasets/test.json.template"), new FileOutputStream(csvPath + File.separator + "test.json"));
		File stagedFile = new File(csvPath + File.separator + "staged" + File.separator + "test.csv.1");
		if (stagedFile.exists()) {
			FileUtils.forceDeleteOnExit(stagedFile);
		}

		when(datasetConf.getLocalPath()).thenReturn(csvPath);

		Whitebox.setInternalState(datasetUploadTasklet, "serviceRequestId", jobExecutionId);
	}

	@Test
	public void execute() throws Exception {
		String schema = "employee";
		StagingJobRequest stagingJobRequest = StagingJobRequest.builder()
				.name("staging")
				.item(StagingItemRequest.builder()
						.dataset(DatasetMetadata.of(schema))
						.source(DataSources.csv("test.json"))
						.build())

				.build();
		JobExecution jobExecution = new JobExecution(jobExecutionId);
		jobExecution.setStatus(BatchStatus.STARTING);

		StepExecution stepExecution = new StepExecution("1", jobExecution);
		StepContext stepContext = new StepContext(stepExecution);
		StagingDataset stagingDataset = new StagingDataset();
		stagingDataset.setId(1l);
		stagingDataset.setStagingFile("/datasets/employee.csv");
		DatasetMetadata datasetMetadata = DatasetMetadata.of(schema);

		when(stagingDatasetRepository.findByJobExecutionIdAndDatasetNameAndMetadataKey(jobExecution.getId(), datasetMetadata.getName()
				, datasetMetadata.toMetadataKey())).thenReturn(stagingDataset);
		when(chunkContext.getStepContext()).thenReturn(stepContext);
		when(stagingDatasetRepository.save(isA(StagingDataset.class))).thenReturn(stagingDataset);
		when(fileSystemTemplate.copy(isA(File.class), isA(Path.class), anyBoolean())).thenReturn(true);

		ServiceRequest serviceRequest = new ServiceRequest();
		serviceRequest.setId(jobExecutionId);
		serviceRequest.setRequestMessage(MapperWrapper.MAPPER.writeValueAsString(stagingJobRequest));

		when(serviceRequestRepository.findOne(jobExecutionId)).thenReturn(serviceRequest);
		datasetUploadTasklet.execute(stepContribution, chunkContext);

		verify(stagingDatasetRepository, new Times(2)).save(isA(StagingDataset.class));
	}

}