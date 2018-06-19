package etl.server.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import etl.server.IgnisApplication;
import etl.api.dq.DataQualityCheckState;
import etl.api.job.JobExecution;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.job.staging.StagingJobRequest;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Map;
import etl.api.table.Table;
import etl.common.json.MapperWrapper;
import etl.server.service.pipeline.impl.job.PipelineConf;
import etl.server.utils.ResourceUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = IgnisApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class DataQualityControllerJobIT extends AbstractJobIT {

	@Autowired
	private PipelineConf pipelineConf;

	@Before
	@Override
	public void init() throws Exception {
		super.init();

		fileSystemTemplate.delete(new Path(datasetConf.getRemotePath()), true);
		fileSystemTemplate.delete(new Path(pipelineConf.getStagingPath()), true);

		// dataset csv will be moved to staged folder after staging
		prepareDatasetCsv();

		dropTable(SCHEMA_NAME);
		dropTable("EmployeeTooYong");
	}

	@After
	public void cleanup() throws Exception {
		dropAllHiveTables();
	}

	@Test
	public void startJob_completed_noError() throws Exception {
		// staging
		doStaging();

		// dq
		PipelineJobRequest jobRequest = createDQJobRequest();
		Long dqExecutionId = submitJob(jobRequest, URL_START_DQ_JOB);

		await().pollInterval(2, TimeUnit.SECONDS).atMost(120, TimeUnit.SECONDS).until(() -> {
			List results = restTemplate.getForObject(String.format(URL_GET_DQ_JOB, dqExecutionId), List.class);
			List<LinkedHashMap<String, String>> dqCheckExecutions = (List<LinkedHashMap<String, String>>) results;
			return javaslang.collection.List.ofAll(dqCheckExecutions).forAll(
					ruleInst -> ruleInst.get("status").equalsIgnoreCase(DataQualityCheckState.CHECKED.toString()));
		});

		// assert dq result
		assertThat(tableExists("EmployeeTooYong")).isTrue();
	}

	private void doStaging() throws Exception {
		// upload schema

		Table employeeTable = createEmployeeSchema();

		tableService.saveOrUpdate(employeeTable);
		StagingJobRequest jobRequest = createEmployeeStagingJobRequest(employeeTable, "employee2.csv");
		long jobExecutionId = submitJob(jobRequest, URL_START_STAGING_JOB);

		await().pollInterval(2, TimeUnit.SECONDS).atMost(300, TimeUnit.SECONDS).until(() -> {
			JobExecution jobExecution = restTemplate.getForObject(String.format(URL_GET_STAGING_JOB, jobExecutionId), JobExecution.class);
			return jobExecution.getJobStatus().equalsIgnoreCase(BatchStatus.COMPLETED.name());
		});

	}
	private Long submitJob(Object jobRequest, String url) throws JsonProcessingException {
		String jobContent = MapperWrapper.MAPPER.writeValueAsString(jobRequest);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<String> entity = new HttpEntity<>(jobContent, headers);

		ResponseEntity<Long> response = restTemplate.exchange(
				url,
				HttpMethod.POST,
				entity,
				Long.class);
		return response.getBody();
	}

	private PipelineJobRequest createDQJobRequest() {
		Map map = Map.builder()
				.name("map")
				.input(DatasetRefs.datasetRef(SCHEMA_NAME, "age < 18"))
				.output(DatasetRef.builder()
						.name("EmployeeTooYong")
						.build())
				.build();

		return PipelineJobRequest.builder()
				.name("PipelineTestJob")
				.step(PipelineStep.builder()
								.name(STEP_NAME)
								.transform(map)
								.build())
				.build();
	}

	private void prepareDatasetCsv() throws IOException, URISyntaxException {
		File templateFile = ResourceUtil.getResourceAsFile("datasets/employee2.csv.template");
		File datasetFile = new File(StringUtils.appendIfMissing(datasetConf.getLocalPath(), File.separator) + "employee2.csv");
		FileUtils.copyFile(templateFile, datasetFile);
	}

}