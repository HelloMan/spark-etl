package etl.server.controller;

import etl.api.dataset.Dataset;
import etl.api.dataset.DatasetMetadata;
import etl.api.parameter.Parameters;
import etl.common.json.MapperWrapper;
import etl.server.exception.dataset.DatasetNotFoundException;
import etl.server.service.dataset.DatasetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@SuppressWarnings("squid:UndocumentedApi")
@RestController
@RequestMapping(value = "/v1/datasets")
@Slf4j
public class DatasetController {

    @Autowired
    private DatasetService datasetService;

    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Dataset createDataset(@RequestBody  Dataset dataset){
        return datasetService.createDataset(dataset);
    }

    @RequestMapping(
            path = "/{datasetName}/lastVersion",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Dataset getLastVersionOfDataset(@PathVariable String datasetName, @RequestParam("metadata")String metadata) throws IOException {
		Parameters parameters = MapperWrapper.MAPPER.readValue(metadata, Parameters.class);
        DatasetMetadata datasetMetadata = new DatasetMetadata(datasetName, parameters.getParameters().values());
        return datasetService.findLastDataset(datasetMetadata)
                .orElseThrow(() -> new DatasetNotFoundException(String.format("No dataset found for [%s]", datasetMetadata)));
    }



}