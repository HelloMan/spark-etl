package etl.api.job.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import etl.api.pipeline.Transform;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Builder
@EqualsAndHashCode(of = {"name"})
public class PipelineStep implements Serializable {

    @NonNull
    private String name;

    private String description;

    @Singular @NonNull
    private List<Transform> transforms;

    @Tolerate
    public PipelineStep( ) {}

}
