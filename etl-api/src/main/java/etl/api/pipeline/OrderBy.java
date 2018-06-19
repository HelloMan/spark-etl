package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Tolerate;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
@EqualsAndHashCode(of = "fieldName")
public class OrderBy implements Serializable {
	
	private String fieldName;
	
	private boolean asc;

	@Builder
	public OrderBy(@NonNull String fieldName, boolean asc) {
		this.fieldName = fieldName;
		this.asc = asc;
	}
}