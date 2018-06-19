package etl.server.service.staging.impl;

import com.google.common.collect.ImmutableList;
import etl.api.job.JobExecution;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.api.dataset.DatasetMetadata;
import etl.api.dataset.DatasetState;
import etl.server.domain.entity.ServiceRequest;
import etl.server.domain.entity.StagingDataset;
import etl.api.datasource.DataSources;
import etl.api.table.Table;
import etl.common.hdfs.FileSystemTemplate;
;

import etl.server.exception.job.JobExecutionStopException;
import etl.server.exception.job.JobStartException;
import etl.server.exception.job.NoSuchJobExecutionException;
import etl.server.repository.ServiceRequestRepository;
import etl.server.repository.StagingDatasetRepository;
import etl.server.service.common.JobOperator;
import etl.server.service.staging.impl.job.DatasetConf;
import etl.server.service.table.TableService;
import javaslang.Tuple2;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.domain.Page;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StagingDatasetServiceImplTest {


	@Mock
	private ServiceRequestRepository serviceRequestRepository;

	@Mock
	private StagingDatasetRepository stagingDatasetRepository;

	@Mock
	private DatasetConf datasetConf;

	@Mock
	private FileSystemTemplate fileSystemTemplate;

	@Mock
	private TableService tableService;

	@InjectMocks
	private StagingDatasetServiceImpl stagingDatasetService;

	private StagingDataset stagingDataset;

	private List<StagingDataset> stagingDatasetList;

	private Page<StagingDataset> page = mock(Page.class);

	@Before
	public void before() {
		JobOperator jobOperator = new JobOperator() {
			@Override
			public long startJob(String jobName, Supplier<ServiceRequest> serviceRequestSupplier, Properties properties) throws JobStartException {
				ServiceRequest serviceRequest = serviceRequestSupplier.get();
				serviceRequest.setId(1l);
				return serviceRequest.getId();
			}

			@Override
			public boolean stop(long executionId) throws JobExecutionStopException {
				return false;
			}

			@Override
			public JobExecution getJobExecution(Long executionId) throws NoSuchJobExecutionException {
				return null;
			}
		};
		Whitebox.setInternalState(stagingDatasetService,"jobOperator",jobOperator);
		stagingDataset = new StagingDataset();
		stagingDataset.setTable("test");
		stagingDataset.setValidationErrorFile("errorFile.csv");

		stagingDataset.setJobExecutionId(1l);

		stagingDatasetList = ImmutableList.of(stagingDataset);

		when(datasetConf.getLocalPath()).thenReturn("/");
		when(datasetConf.getRemotePath()).thenReturn("/remotePath");
		page = mock(Page.class);
		when(page.getTotalElements()).thenReturn(2l);
		when(page.getTotalPages()).thenReturn(1);
		when(page.getContent()).thenReturn(stagingDatasetList);
		when(page.getNumber()).thenReturn(1);
		when(page.getNumberOfElements()).thenReturn(2);
		when(page.getSize()).thenReturn(1);




	};

	@Test
	public void startJob() throws Exception {
		ServiceRequest serviceRequest = new ServiceRequest();
		serviceRequest.setId(1l);

		StagingJobRequest stagingJobRequest = StagingJobRequest.builder()
				.name("staging")
				.item(StagingItemRequest.builder()
						.table("employee")
						.dataset(DatasetMetadata.of("employee"))
						.source(DataSources.csv("test.csv"))
						.build())
				.build();
		when(tableService.findTable(isA(String.class))).thenReturn(Optional.of(new Table()));


		when(serviceRequestRepository.save(isA(ServiceRequest.class))).thenReturn(serviceRequest);
		long result = stagingDatasetService.start(stagingJobRequest);
		assertThat(result).isEqualTo(1l);
	}

	@Test
	public void findStagingDataset() {

		when(stagingDatasetRepository.findOne(1l)).thenReturn(new StagingDataset());
		assertThat(stagingDatasetService.findStagingDataset(1l)).isNotNull();
	}


	@Test
	public void findStagingDatasets() throws Exception {
		when(stagingDatasetRepository.findByJobExecutionIdAndDatasetName(1l, "employee")).thenReturn(ImmutableList.of(new StagingDataset()));
		when(stagingDatasetRepository.findByJobExecutionId(1l )).thenReturn(ImmutableList.of(new StagingDataset(),new StagingDataset()));

		assertThat(stagingDatasetService.findStagingDatasets(1l, "employee").size()).isEqualTo(1);
		assertThat(stagingDatasetService.findStagingDatasets(1l, null).size()).isEqualTo(2);

	}


	@Test
	public void getDatasetErrorFile() throws Exception {
		when(stagingDatasetRepository.findOne(1l)).thenReturn(stagingDataset);
		ByteArrayInputStream inputStream = new ByteArrayInputStream("xxxInputStream".getBytes());

		when(fileSystemTemplate.open(new Path(stagingDataset.getValidationErrorFile()))).thenReturn(inputStream);
		Optional<Tuple2<String, InputStream>> actualResult = stagingDatasetService.getStagingDatasetError(1l);
		assertThat(actualResult.isPresent()).isTrue();
		assertThat(actualResult.get()._1).isEqualTo("errorFile.csv");
		assertThat(IOUtils.toString(actualResult.get()._2)).isEqualTo("xxxInputStream");
	}

	@Test
	public void updateDatasetState() throws Exception {

		DatasetState state = DatasetState.UPLOADING;
		stagingDataset.setStatus(DatasetState.VALIDATING);

		when(stagingDatasetRepository.findOne(1l)).thenReturn(stagingDataset);
		when(stagingDatasetRepository.save(stagingDataset)).thenReturn(stagingDataset);

		stagingDatasetService.updateStagingDatasetState(1l, DatasetState.VALIDATED.name());

		verify(stagingDatasetRepository).findOne(1l);
		verify(stagingDatasetRepository).save(stagingDataset);
	}


}