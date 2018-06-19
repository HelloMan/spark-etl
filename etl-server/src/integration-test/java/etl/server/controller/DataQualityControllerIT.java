package etl.server.controller;


import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import etl.server.IgnisApplication;
import etl.api.dq.DataQualityCheckExecution;
import etl.api.dq.DataQualityCheckState;
import etl.common.hdfs.FileSystemTemplate;
import etl.server.repository.DQRuleInstanceRepository;
import org.awaitility.Duration;
import org.awaitility.core.ConditionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.pollinterval.FibonacciPollInterval.fibonacci;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = IgnisApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class DataQualityControllerIT {
    private static final String URL_GET_JOB = "/v1/dq/jobs/%d";
    private static final String URL_START_JOB = "/v1/dq/jobs";
    private static final String URL_GET_RULE_REJECTIONS = "/v1/dq/jobs/%d/rules/%s/rejections";
    private static final String URL_GET_RULE_EXECUTIONS = "/v1/dq/jobs/%d/rules";
    private static final String URL_UPDATE_RULE_INSTANCE = "/v1/dq/jobs/%d/rules/%s";
    public static final ConditionFactory WAIT = await()
            .atMost(20, SECONDS)
            .pollInterval(fibonacci(SECONDS))
            .pollDelay(Duration.ONE_SECOND);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private DQRuleInstanceRepository dqRuleInstanceRepository;

    public static final String FILE_PATH = "/datasets/schema/test.txt";

    private List<DataQualityCheckExecution> dqRuleList = new ArrayList<>();

    private HttpMessageConverter mappingJackson2HttpMessageConverter;

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @Autowired
    void setConverters(HttpMessageConverter<?>[] converters) {

        this.mappingJackson2HttpMessageConverter = Arrays.asList(converters).stream()
                .filter(hmc -> hmc instanceof MappingJackson2HttpMessageConverter)
                .findAny()
                .orElse(null);

        assertNotNull("the JSON message converter must not be null",
                this.mappingJackson2HttpMessageConverter);
    }

    @Before
    public void setup() throws Exception {
        dqRuleInstanceRepository.deleteAll();
        dqRuleList.add(createNewDQRuleInstance("rule1", 1L));
        dqRuleList.add(createNewDQRuleInstance("rule2", 1L));
        dqRuleList.add(createNewDQRuleInstance("RULE", 2L));

    }

    @Test
    public void testUpdateRuleInstance() throws Exception {
        DataQualityCheckExecution dqCheckExecution = dqRuleList.get(2);
        dqCheckExecution.setFailedRecordNumber(100);
        dqCheckExecution.setStatus(DataQualityCheckState.CHECKING);
        //act
        restTemplate.put(String.format(URL_UPDATE_RULE_INSTANCE, 2, "RULE"), dqCheckExecution);

        //verify
        DataQualityCheckExecution updateRuleInstance = dqRuleInstanceRepository.findByJobExecutionIdAndName(2L, "RULE");
        assertNotNull(updateRuleInstance);
        assertThat(updateRuleInstance.getStatus()).isEqualTo(DataQualityCheckState.CHECKING);
        assertThat(updateRuleInstance.getFailedRecordNumber()).isEqualTo(100L);

    }

    @Test
    public void testGetRuleExecutions() throws Exception {

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<String> response
                = restTemplate.exchange(String.format(URL_GET_RULE_EXECUTIONS, 1), HttpMethod.GET, entity, String.class);

        DocumentContext documentContext = JsonPath.parse(response.getBody());

        assertThat((String) documentContext.read("$[0].targetFile")).isEqualTo(dqRuleList.get(0).getTargetFile());
        assertThat((Integer) documentContext.read("$[0].jobExecutionId")).isEqualTo(Long.valueOf(dqRuleList.get(0).getJobExecutionId()).intValue());
        assertThat((String) documentContext.read("$[0].name")).isEqualTo(dqRuleList.get(0).getName());
        assertThat((String) documentContext.read("$[1].name")).isEqualTo(dqRuleList.get(1).getName());
    }

    @Test
    public void testGetRejections() throws Exception {
        String dqCheckExecutionName = "rule1";

        String testFile = "test file content\n welcome!";
        InputStream stream = new ByteArrayInputStream(testFile.getBytes(StandardCharsets.UTF_8));
        fileSystemTemplate.copy(stream, FILE_PATH);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<>(headers);
        //act
        ResponseEntity<String> response = this.restTemplate
                .exchange(String.format(URL_GET_RULE_REJECTIONS, 1, dqCheckExecutionName), HttpMethod.GET, entity, String.class);
        //assert
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .isEqualTo("attachment; filename=test.txt");

        assertThat(response.getBody()).isEqualTo(testFile);
    }

    private DataQualityCheckExecution createNewDQRuleInstance(String dqCheckExecutionName, Long jobExecutionId) {

        DataQualityCheckExecution dqCheckExecution = new DataQualityCheckExecution();
        dqCheckExecution.setStartTime(new Date());
        dqCheckExecution.setEndTime(new Date());
        dqCheckExecution.setName(dqCheckExecutionName);
        dqCheckExecution.setJobExecutionId(jobExecutionId);
        dqCheckExecution.setTargetFile(FILE_PATH);
        dqCheckExecution.setStatus(DataQualityCheckState.ACCEPTED);
        return dqRuleInstanceRepository.save(dqCheckExecution);
    }

    protected String json(Object o) throws IOException {
        MockHttpOutputMessage mockHttpOutputMessage = new MockHttpOutputMessage();
        this.mappingJackson2HttpMessageConverter.write(
                o, MediaType.APPLICATION_JSON, mockHttpOutputMessage);
        return mockHttpOutputMessage.getBodyAsString();
    }
}