package etl.server.controller;

import etl.api.job.JobExecution;
import etl.server.exception.job.JobExecutionStopException;
import etl.server.exception.job.NoSuchJobExecutionException;
import etl.server.service.common.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

public abstract class AbstractJobController {

    @Autowired
    protected JobOperator jobOperator;

    @RequestMapping(value = "/jobs/{jobExecutionId}/stop",
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public boolean terminateJob(@PathVariable("jobExecutionId") long jobExecutionId)
            throws JobExecutionStopException {
        return jobOperator.stop(jobExecutionId);
    }

    @RequestMapping(value = "/jobs/{jobExecutionId}", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public JobExecution getJob(@PathVariable("jobExecutionId") long jobExecutionId) throws NoSuchJobExecutionException {
        return jobOperator.getJobExecution(jobExecutionId);
    }
}
