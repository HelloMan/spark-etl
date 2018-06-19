package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
public abstract class SingleInOutTransform extends SingleOutTransform {

	private DatasetRef input;

	public SingleInOutTransform(String name, DatasetRef input,DatasetRef output) {
		super(name,output);
		this.input = input;
	}

	@Override
	@JsonIgnore
	public List<DatasetRef> getTransformInputs() {
		return ImmutableList.of(input);
	}
}