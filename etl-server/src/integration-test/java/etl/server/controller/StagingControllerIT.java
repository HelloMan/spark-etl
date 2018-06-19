package etl.server.controller;

import etl.server.IgnisApplication;
import etl.api.dataset.DatasetPath;
import etl.api.job.JobExecution;
import etl.api.job.staging.StagingJobRequest;
import etl.api.table.Table;
import etl.server.utils.ResourceUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = IgnisApplication.class,webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@AutoConfigureMockMvc
public class StagingControllerIT extends AbstractJobIT {

    File datasetFile;

    @Before
    public void setup() throws Exception {
        super.init();
        //remove hdfs staging path form employee schema
        fileSystemTemplate.delete(new Path(datasetConf.getRemotePath()), true);
        //prepare dataset file
        datasetFile = new File(StringUtils.appendIfMissing(datasetConf.getLocalPath(), File.separator) + "employee.csv");
        FileUtils.forceDeleteOnExit(datasetFile);
        dropTable(SCHEMA_NAME);
    }

    @Test
    public void testStartShouldCompleted() throws Exception {
        //arrange upload table
        Table schema = createEmployeeSchema();
        tableService.saveOrUpdate(schema);

        FileUtils.copyInputStreamToFile(ResourceUtil.getResourceAsStream("datasets/employee.csv.template"), datasetFile);

        StagingJobRequest stagingJobRequest = createEmployeeStagingJobRequest(schema, "employee.csv");

        long jobExecutionId = restTemplate.postForObject(URL_START_STAGING_JOB, stagingJobRequest, Long.class);

        await().pollInterval(2, TimeUnit.SECONDS).atMost(5, TimeUnit.MINUTES).untilAsserted(() -> {
            JobExecution jobExecution = restTemplate.getForObject(String.format(URL_GET_STAGING_JOB, jobExecutionId), JobExecution.class);
            assertThat(jobExecution.getJobStatus()).isEqualToIgnoringCase(BatchStatus.COMPLETED.name());
        });

		DatasetPath datasetPath = new DatasetPath(jobExecutionId, SCHEMA_NAME, datasetConf.getRemotePath());
		assertThat(fileSystemTemplate.exists(new Path(datasetPath.getStagingErrorFile()))).isTrue();
    }

	@After
    public void cleanUp(){
		dropAllHiveTables();
    }

}