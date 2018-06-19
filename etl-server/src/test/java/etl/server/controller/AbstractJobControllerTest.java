package etl.server.controller;

import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.job.JobStartException;
import etl.server.repository.ServiceRequestRepository;
import etl.server.service.common.JobOperator;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static etl.common.json.MapperWrapper.MAPPER;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


public abstract class AbstractJobControllerTest {
    @MockBean
    private JobExplorer jobExplorer;

    @Autowired
    protected MockMvc mvc;


    @MockBean
    protected JobOperator jobOperator;

    @MockBean private ServiceRequestRepository serviceRequestRepository;

    @Test
    public void startJob_ShouldPassAndReturnHttp200() throws Exception {
        Object jobRequest = prepareStartJob();

        mvc.perform(MockMvcRequestBuilders
                .post(getStartJobUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .content(MAPPER.writeValueAsString(jobRequest)))
                .andExpect(status().isOk());
    }

    protected abstract Object prepareStartJob() throws JobStartException;

    @Test
    public void terminateJob_ShouldPassAndReturnHttp200() throws Exception {


        when(jobOperator.stop(anyLong())).thenReturn(true);
        mvc.perform(MockMvcRequestBuilders
                .put(getTerminateJobUrl())
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }


    @Test
    public void getJob_ShouldPassAndReturnHttp200() throws Exception {
        ServiceRequest serviceRequest = new ServiceRequest();
        serviceRequest.setJobExecutionId(1l);

        when(serviceRequestRepository.findOne(anyLong())).thenReturn(serviceRequest);
        JobExecution jobExecution = new JobExecution(1l);
        jobExecution.setStatus(BatchStatus.COMPLETED);
        given(jobExplorer.getJobExecution(anyLong()))
                .willReturn(jobExecution);

        this.mvc.perform(MockMvcRequestBuilders
                .get(getJobUrl()))
                .andExpect(status().isOk());
    }

    protected abstract String getStartJobUrl();

    protected abstract String getTerminateJobUrl();

    protected abstract String getJobUrl();
}
