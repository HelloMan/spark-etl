package etl.server.service.pipeline.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import etl.api.dataset.DatasetMetadata;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.job.pipeline.PipelineStepHelper;
import etl.api.pipeline.DatasetRef;
import etl.common.json.MapperWrapper;
import etl.server.domain.entity.ServiceRequestType;
import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.job.JobStartException;
import etl.server.repository.ServiceRequestRepository;
import etl.server.service.common.JobOperator;
import etl.server.service.dataset.DatasetService;
import etl.server.service.pipeline.PipelineJobService;
import etl.server.service.table.TableService;
import javaslang.Tuple2;
import one.util.streamex.StreamEx;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;
import java.util.Properties;

public abstract class AbstractPipelineJobService implements PipelineJobService {
    @Autowired
    private JobOperator jobOperator;

    @Autowired
    protected TableService tableService;

    @Autowired
    protected ServiceRequestRepository serviceRequestRepository;

    @Autowired
    protected DatasetService datasetService;

    public long start(PipelineJobRequest jobRequest) throws JobStartException {
		checkDataset(jobRequest);
        return jobOperator.startJob(getTransformationType().name(), () -> this.createPipelineJob(jobRequest), new Properties());
    }

    protected ServiceRequest createPipelineJob(PipelineJobRequest jobRequest) {
        ServiceRequest serviceRequest = new ServiceRequest();
        serviceRequest.setName(jobRequest.getName());
        serviceRequest.setServiceRequestType(ServiceRequestType.PIPELINE);

        try {
            serviceRequest.setRequestMessage(MapperWrapper.MAPPER.writeValueAsString(jobRequest));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
        return serviceRequestRepository.save(serviceRequest);
    }

	private void checkDataset(PipelineJobRequest job) throws JobStartException {
		for (PipelineStep step : job.getSteps()) {
			PipelineStepHelper stepHelper = new PipelineStepHelper(step);
			Optional<Tuple2<DatasetRef, DatasetMetadata>> datasetNotFound = StreamEx.of(step.getTransforms())
					.flatMap(t -> StreamEx.of(t.getTransformInputs()))
					.filter(ref -> !stepHelper.isImmediateDatasetRef(ref))
					.map(ref -> new Tuple2<>(ref, ref.toDatasetMetadata(job.getParameters())))
					.findFirst(metadata -> !datasetService.findLastDataset(metadata._2).isPresent());

			if (datasetNotFound.isPresent()) {
				Tuple2<DatasetRef, DatasetMetadata> t = datasetNotFound.get();
				throw new JobStartException(
						String.format("Unable to find dataset: %s by metadata: %s", t._1.getName(), t._2));
			}
		}
	}



}