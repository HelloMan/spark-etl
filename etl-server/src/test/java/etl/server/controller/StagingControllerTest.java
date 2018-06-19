package etl.server.controller;

import com.google.common.collect.ImmutableList;
import etl.server.TestIgnisApplication;
import etl.api.dataset.DatasetState;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.server.domain.entity.StagingDataset;
import etl.server.exception.job.JobStartException;
import etl.server.service.staging.StagingDatasetService;
import javaslang.Tuple;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = TestIgnisApplication.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StagingControllerTest extends AbstractJobControllerTest {

    private static final String URL_START_JOB = "/v1/staging/jobs";
    private static final String URL_GET_JOB = "/v1/staging/jobs/%d";
    private static final String URL_TERMINATE_JOB = "/v1/staging/jobs/%d/stop";
    private static final String URL_GET_STAGING_DATASET_BY_ID = "/v1/staging/datasets/%d";

    private static final String URL_GET_STAGING_DATASETS_BY_JOB_ID = "/v1/staging/jobs/%d/datasets?dataset=%s";

    private static final String URL_GET_STAGING_DATASETS_ERROR_FILE_BY_ID = "/v1/staging/datasets/%d/validationError";
    private static final String URL_UPDATE_STAGING_DATASET_BY_ID = "/v1/staging/datasets/%d?state=%s&predicate=%s&recordsCount=%d";






    @MockBean
    private StagingDatasetService stagingDatasetService;



    @Override
    protected Object prepareStartJob() throws JobStartException {
        StagingJobRequest stagingJobRequest = StagingJobRequest.builder()
                .name("staging")
                .item(StagingItemRequest.builder()
                        .table("desc") .build() )
                .build();

        given(this.stagingDatasetService.start(any(StagingJobRequest.class))).willReturn(1L);
        return stagingJobRequest;

    }


    @Override
    protected String getStartJobUrl() {
        return URL_START_JOB;
    }

    @Override
    protected String getTerminateJobUrl() {
        return String.format(URL_TERMINATE_JOB, 1);
    }

    @Override
    protected String getJobUrl() {
        return String.format(URL_GET_JOB, 1l);
    }

    @Test
    public void testFindStagingDataset() throws Exception {
        when(stagingDatasetService.findStagingDataset(1)).thenReturn(Optional.of(new StagingDataset()));
        this.mvc.perform(MockMvcRequestBuilders
                .get(String.format(URL_GET_STAGING_DATASET_BY_ID, 1))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testFindStagingDatasets() throws Exception {
        when(stagingDatasetService.findStagingDatasets(1, "employee")).thenReturn(ImmutableList.of(new StagingDataset()));

        this.mvc.perform(MockMvcRequestBuilders
                .get(String.format(URL_GET_STAGING_DATASETS_BY_JOB_ID, 1, "employee"))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testDownloadValidationErrorFile() throws Exception {

        when(stagingDatasetService.getStagingDatasetError(1))
                .thenReturn(Optional.of(Tuple.of("employee", new ByteArrayInputStream("s".getBytes()))));
        this.mvc.perform(MockMvcRequestBuilders
                .get(String.format(URL_GET_STAGING_DATASETS_ERROR_FILE_BY_ID, 1))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    public void testUpdateDataSetState() throws Exception {

        doNothing().when(stagingDatasetService).updateStagingDatasetState(1l, DatasetState.UPLOADING.name());
        this.mvc.perform(MockMvcRequestBuilders
                .put(String.format(URL_UPDATE_STAGING_DATASET_BY_ID, 1, DatasetState.UPLOADING.name(), "", 0))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }
}
