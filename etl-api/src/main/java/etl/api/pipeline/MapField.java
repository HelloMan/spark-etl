package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public final class MapField extends Field {
	private FieldType fieldType;

	@Builder
	private MapField(@NonNull String name,	String as,@NonNull FieldType fieldType) {
		super(name, as);
		this.fieldType = fieldType;
	}



}