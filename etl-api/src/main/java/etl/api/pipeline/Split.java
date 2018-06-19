package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Getter
public class Split extends Transform {
	
	private DatasetRef input;
	private DatasetRef output1;
	private DatasetRef output2;
	
	private String condition;
	
	@Builder
	public Split(@NonNull String name,
				 @NonNull DatasetRef input,
				 @NonNull DatasetRef output1,
				 @NonNull DatasetRef output2,
				 @NonNull String condition) {
		super(name);
		this.input = input;
		this.output1 = output1;
		this.output2 = output2;
		this.condition = condition;
	}


	@Override
	@JsonIgnore
	public boolean isSingleOutput() {
		return false;
	}

	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public List<DatasetRef> getTransformInputs() {
		return ImmutableList.of(input);
	}

	@Override
	public List<DatasetRef> getTransformOutputs() {
		return ImmutableList.of(output1,output2);
	}
}