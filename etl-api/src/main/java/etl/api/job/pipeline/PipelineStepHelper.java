package etl.api.job.pipeline;

import com.google.common.collect.ImmutableList;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.SingleOutTransform;
import etl.api.pipeline.Split;
import etl.api.pipeline.Transform;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public final class PipelineStepHelper {

    private final Set<DatasetRef> immediateDatasets;

    private final PipelineStep pipelineStep;

    public PipelineStepHelper(PipelineStep pipelineStep) {
        this.pipelineStep = pipelineStep;
        immediateDatasets = buildImmediateDatasets(pipelineStep);
    }

    private Set<DatasetRef> buildImmediateDatasets(PipelineStep pipelineStep) {

        Set<DatasetRef> immediateDatasetRefs = pipelineStep.getTransforms().stream()
                .filter(transform1 -> CollectionUtils.isEmpty(transform1.getTransformOutputs()))
                .flatMap(this::getImmediateDatasetByTransform)
                .collect(Collectors.toSet());


        return pipelineStep.getTransforms().stream()
                .flatMap(transform -> transform.getTransformInputs().stream())
                .filter(immediateDatasetRefs::contains)
                .collect(Collectors.toSet());
    }

    private Stream<? extends DatasetRef> getImmediateDatasetByTransform(Transform transform) {
        ImmutableList.Builder<DatasetRef> result = ImmutableList.builder();
        if (transform.isSingleOutput()) {
            result.add(DatasetRefs.fromTransform((SingleOutTransform) transform));
        } else {
            result.add(DatasetRefs.fromTransform((Split) transform, true));
            result.add(DatasetRefs.fromTransform((Split) transform, false));
        }
        return result.build().stream();
    }

    public boolean isImmediateDatasetRef(DatasetRef datasetRef) {
        return immediateDatasets.contains(datasetRef);
    }
}
