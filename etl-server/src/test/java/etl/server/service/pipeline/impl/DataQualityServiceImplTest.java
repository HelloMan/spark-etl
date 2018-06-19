package etl.server.service.pipeline.impl;

import com.google.common.collect.ImmutableList;
import etl.api.dq.DataQualityCheckExecution;
import etl.api.dq.DataQualityCheckState;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Distinct;
import etl.common.hdfs.FileSystemTemplate;
import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.dq.NoSuchRuleException;
import etl.server.repository.DQRuleInstanceRepository;
import etl.server.repository.ServiceRequestRepository;
import javaslang.Tuple2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)

public class DataQualityServiceImplTest {

    @InjectMocks
    DataQualityServiceImpl dataQualityService;

    @Mock
    private DQRuleInstanceRepository dqRuleInstanceRepository;

    @Mock
    private FileSystemTemplate fileSystemTemplate;

	@Mock
	private ServiceRequestRepository serviceRequestRepository;

	@Test
	public void createPipelineJob() {
		PipelineJobRequest job = PipelineJobRequest.builder()
				.name("pipelineJob")
				.step(PipelineStep.builder()
						.name("step")
						.transform(Distinct.builder().name("distinct").input(DatasetRefs.datasetRef("employee")).build())
						.build())
				.build();

		ServiceRequest serviceRequest = new ServiceRequest();
		serviceRequest.setId(1L);

		when(serviceRequestRepository.save(any(ServiceRequest.class))).thenReturn(serviceRequest);

		dataQualityService.createPipelineJob(job);
		verify(dqRuleInstanceRepository).save(isA(DataQualityCheckExecution.class));
	}

    @Test(expected = NoSuchRuleException.class)
    public void updateDataQualityRuleExecution_shouldThrowNoSuchRuleException() throws Exception {
        dataQualityService.updateDataQualityRuleExecution(1l, "rule", new DataQualityCheckExecution());
    }

    @Test
    public void updateDataQualityRuleExecution() throws Exception {
		DataQualityCheckExecution dqCheckExecution = new DataQualityCheckExecution();
		dqCheckExecution.setStatus(DataQualityCheckState.ACCEPTED);
		when(dqRuleInstanceRepository.findByJobExecutionIdAndName(1l, "rule")).thenReturn(dqCheckExecution);

		DataQualityCheckExecution ruleExecution = new DataQualityCheckExecution();
		ruleExecution.setStatus(DataQualityCheckState.CHECKING);
		dataQualityService.updateDataQualityRuleExecution(1l, "rule", ruleExecution);

        verify(dqRuleInstanceRepository).findByJobExecutionIdAndName(1l, "rule");
        verify(dqRuleInstanceRepository).save(isA(DataQualityCheckExecution.class));
    }


    @Test
    public void getDataQualityRuleExecutions() throws Exception {
        when(dqRuleInstanceRepository.findByJobExecutionId(1l)).thenReturn(ImmutableList.of(new DataQualityCheckExecution()));
        List<DataQualityCheckExecution> result = dataQualityService.getDataQualityRuleExecutions(1l);
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void getResultOfDataQualityRuleExecution() throws Exception {
        DataQualityCheckExecution dqCheckExecution = new DataQualityCheckExecution();
        dqCheckExecution.setTargetFile("targetFile");

        when(dqRuleInstanceRepository.findByJobExecutionIdAndName(1l, "rule")).thenReturn(dqCheckExecution);

        Optional<Tuple2<String, InputStream>> result =  dataQualityService.getResultOfDataQualityRuleExecution(1l, "rule");
        assertThat(result.isPresent()).isTrue();

    }



}