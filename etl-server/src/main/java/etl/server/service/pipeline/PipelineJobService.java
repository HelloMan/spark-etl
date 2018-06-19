package etl.server.service.pipeline;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.server.domain.TransformationType;
import etl.server.exception.job.JobStartException;

public interface PipelineJobService {
    long start(PipelineJobRequest jobRequest) throws JobStartException;

    TransformationType getTransformationType();
}
