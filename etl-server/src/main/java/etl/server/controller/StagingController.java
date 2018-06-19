package etl.server.controller;

import etl.api.job.staging.StagingJobRequest;
import etl.server.domain.entity.StagingDataset;
import etl.server.exception.job.JobStartException;
import etl.server.exception.staging.DatasetStateChangeException;
import etl.server.exception.staging.NoSuchDatasetException;
import etl.server.service.staging.StagingDatasetService;
import javaslang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("squid:UndocumentedApi")
@RestController
@RequestMapping(value = "/v1/staging")
@Slf4j
public class StagingController extends AbstractJobController {
    @Autowired
    private StagingDatasetService stagingDatasetService;

    @RequestMapping(value = "/jobs",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public long startJob(@RequestBody StagingJobRequest request)
            throws JobStartException {
        return stagingDatasetService.start(request);

    }


    @RequestMapping(value = "/datasets/{datasetId}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public StagingDataset findStagingDataset(
            @PathVariable Long datasetId) throws NoSuchDatasetException {
        return stagingDatasetService.findStagingDataset(datasetId).orElseThrow(() -> new NoSuchDatasetException(datasetId));
    }


    @RequestMapping(value = "/jobs/{jobExecutionId}/datasets",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<StagingDataset> findStagingDatasets(@PathVariable Long jobExecutionId,
                                                        @RequestParam(required = false) String dataset) {
        Objects.requireNonNull(jobExecutionId);
        return stagingDatasetService.findStagingDatasets(jobExecutionId, dataset);
    }



    @RequestMapping(value = "/datasets/{datasetId}/validationError", method = RequestMethod.GET)
    public void downloadValidationErrorFile(
            HttpServletResponse response,
            @PathVariable long datasetId) throws IOException {

        Optional<Tuple2<String, InputStream>> file = stagingDatasetService.getStagingDatasetError(datasetId);

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


    @RequestMapping(value = "/datasets/{datasetId}",
            method = RequestMethod.PUT)
    @ResponseBody
    public void updateDataSetState(@PathVariable long datasetId,
                                   @RequestParam(name = "state") String state) throws
            DatasetStateChangeException {
        stagingDatasetService.updateStagingDatasetState(datasetId, state);

    }



}
