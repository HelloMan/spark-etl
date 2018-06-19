package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Getter
public class Union extends SingleOutTransform {

    private List<DatasetRef> inputs;

	@Builder
	public Union(@NonNull String name,
				 @NonNull @Singular List<DatasetRef> inputs,
				 DatasetRef output) {
		super(name,output);
		this.inputs = Lists.newArrayList(inputs);
	}

	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	@JsonIgnore
	public List<DatasetRef> getTransformInputs() {
		return ImmutableList.copyOf(inputs);
	}
}