package etl.server.controller;

import etl.api.dq.DataQualityCheckExecution;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.server.exception.dq.DQRuleInstanceStatusChangeException;
import etl.server.exception.dq.NoSuchRuleException;
import etl.server.exception.job.JobStartException;
import etl.server.exception.job.NoSuchJobExecutionException;
import etl.server.service.pipeline.DataQualityService;
import javaslang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("squid:UndocumentedApi")
@RestController
@RequestMapping(value = "/v1/dq")
@Slf4j
public class DataQualityController extends AbstractJobController {

    @Autowired
    private DataQualityService dataQualityService;

    @RequestMapping(value = "/jobs",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public long startJob(@RequestBody PipelineJobRequest request)
            throws JobStartException {
        return dataQualityService.start(request);
    }

    @RequestMapping(value = "/jobs/{jobExecutionId}/rules/{ruleName}",
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    public void updateRuleInstance(@PathVariable long jobExecutionId, @PathVariable String ruleName,
                                   @RequestBody DataQualityCheckExecution ruleExecution) throws NoSuchRuleException, DQRuleInstanceStatusChangeException {
        dataQualityService.updateDataQualityRuleExecution(jobExecutionId, ruleName, ruleExecution);
    }

    @RequestMapping(value = "/jobs/{jobExecutionId}/rules",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataQualityCheckExecution> getRuleExecutions(@PathVariable long jobExecutionId) throws NoSuchJobExecutionException {
        return dataQualityService.getDataQualityRuleExecutions(jobExecutionId);
    }

    @RequestMapping(value = "/jobs/{jobExecutionId}/rules/{ruleName}/rejections",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void getRejections(HttpServletResponse response,
                              @PathVariable long jobExecutionId,
                              @PathVariable String ruleName) throws NoSuchJobExecutionException, NoSuchRuleException, IOException {

        Optional<Tuple2<String, InputStream>> file = dataQualityService.getResultOfDataQualityRuleExecution(jobExecutionId, ruleName);

        if (!file.isPresent()) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            return;
        }
        try (InputStream errorFileIs = file.get()._2) {
            response.setContentType("text/csv");
            response.addHeader("Content-Disposition", "attachment; filename=" + file.get()._1);
            IOUtils.copy(errorFileIs, response.getOutputStream());
        }
    }

}