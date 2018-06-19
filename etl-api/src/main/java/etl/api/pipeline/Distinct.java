package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public class Distinct extends SingleInOutTransform {

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private List<String> fields;
	
	@Builder
	public Distinct(@NonNull String name,
					@NonNull DatasetRef input,
					DatasetRef output,
					@Singular List<String> fields) {
		super(name,input,output);
		this.fields = ImmutableList.copyOf(fields);
	}
	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}
}