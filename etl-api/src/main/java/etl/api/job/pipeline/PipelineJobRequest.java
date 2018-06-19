package etl.api.job.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import etl.api.parameter.Parameter;
import etl.api.parameter.Parameters;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor
public class PipelineJobRequest {
    private String name;
    private Set<Parameter> jobParameters;
    private List<PipelineStep> steps;

    @Builder
    public PipelineJobRequest(@NonNull String name,@Singular Set<Parameter> jobParameters,@Singular List<PipelineStep> steps) {
        this.name = name;
        this.jobParameters = jobParameters;
        this.steps = steps;
    }

	@JsonIgnore
	public Parameters getParameters() {
		if (jobParameters == null) {
			return new Parameters();
		} else {
			Map<String, Parameter> parameterMap = jobParameters.stream().collect(Collectors.toMap(Parameter::getName, Function.identity()));
			return new Parameters(parameterMap);
		}
	}

}