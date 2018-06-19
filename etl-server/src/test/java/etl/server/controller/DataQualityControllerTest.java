package etl.server.controller;

import etl.server.TestIgnisApplication;
import etl.api.dq.DataQualityCheckExecution;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.FieldType;
import etl.api.pipeline.Map;
import etl.api.pipeline.MapField;
import etl.api.pipeline.DatasetRefs;
import etl.common.json.MapperWrapper;
import etl.server.exception.job.JobStartException;
import etl.server.service.pipeline.DataQualityService;
import javaslang.Tuple2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.ByteArrayInputStream;
import java.util.Optional;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestIgnisApplication.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class DataQualityControllerTest extends AbstractJobControllerTest {
    private static final String URL_START_JOB = "/v1/dq/jobs";
    private static final String URL_GET_JOB = "/v1/dq/jobs/%d";
    private static final String URL_TERMINATE_JOB = "/v1/dq/jobs/%d/stop";

    private static final String URL_UPDATE_RULE_INSTANCE = "/v1/dq/jobs/%d/rules/%s";

    private static final String URL_GET_RULE_EXECUTIONS = "/v1/dq/jobs/%d/rules";

    private static final String URL_GET_RULE_REJECTIONS = "/v1/dq/jobs/%d/rules/%s/rejections";

    @MockBean
    private DataQualityService dataQualityService;



    @Override
    protected Object prepareStartJob() throws JobStartException {
        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("pipeline")
                .step(PipelineStep.builder()
                        .name("step")
                        .description("desc")
                        .transform(Map.builder()
                                .name("map")
                                .input(DatasetRefs.datasetRef("employee"))
                                .output(DatasetRefs.datasetRef("output"))
                                .field(MapField.builder()
                                        .name("name")
                                        .fieldType(FieldType.of(FieldType.Type.INT))
                                        .build())
                                .build())
                        .build())

                .build();

        given(this.dataQualityService.start(any(PipelineJobRequest.class))).willReturn(1L);
        return pipeline;
    }

    @Override
    protected String getStartJobUrl() {
        return URL_START_JOB;
    }

    @Override
    protected String getTerminateJobUrl() {
        return String.format(URL_TERMINATE_JOB, 1l);
    }

    @Override
    protected String getJobUrl() {
        return String.format(URL_GET_JOB, 1L);
    }


    @Test
    public void updateRuleInstance_ShouldPassAndReturnHttp200() throws Exception{
        DataQualityCheckExecution ruleExecution = new DataQualityCheckExecution();
        this.mvc.perform(MockMvcRequestBuilders
                .put(String.format(URL_UPDATE_RULE_INSTANCE, 1, "RULE"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(MapperWrapper.MAPPER.writeValueAsString(ruleExecution)))
                .andExpect(status().isOk());
    }
    @Test
    public void getRuleExecutions_shouldPassAndReturnHttp200() throws Exception {
        this.mvc.perform(MockMvcRequestBuilders
                .get(String.format(URL_GET_RULE_EXECUTIONS, 1))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void getRejections_shouldPassAndReturnHttp200() throws Exception {
       try( ByteArrayInputStream inputStream = new ByteArrayInputStream("hello".getBytes())){
           when(dataQualityService.getResultOfDataQualityRuleExecution(1l, "RULE")).thenReturn(Optional.of(new Tuple2<>("test", inputStream)));
           this.mvc.perform(MockMvcRequestBuilders
                   .get(String.format(URL_GET_RULE_REJECTIONS, 1,"RULE"))
                   .contentType(MediaType.APPLICATION_JSON))
                   .andExpect(status().isOk());
       }

    }

}