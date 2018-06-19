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
public class Map extends SingleInOutTransform {
	
	/**
	 * In our domain model, a filter is not a transform but is implemented through a Map.<br/>
	 * If 'fields' is an empty list, no mapping is performed.
	 */
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private List<MapField> fields;

	@Builder
	public Map(@NonNull String name,@NonNull DatasetRef input,DatasetRef output,@NonNull @Singular List<MapField> fields) {
		super(name, input, output);
		this.fields = ImmutableList.copyOf(fields);
	}

	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}
}