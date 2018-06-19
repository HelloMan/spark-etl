package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public class JoinField extends Field {
	
	/**
	 * the side of input(input1 or input2)
	 */
	private boolean fromLeftSide = true;
	
	public static JoinField left(String name) {
		return left(name, name);
	}
	public static JoinField left(String name,String as) {
		return JoinField.builder().name(name).as(as).fromLeftSide(true).build();
	}
	public static JoinField right(String name) {
		return right(name, name);
	}

	public static JoinField right(String name,String as) {
		return JoinField.builder().name(name).as(as).fromLeftSide(false).build();
	}
	
	@Builder
	JoinField(@NonNull String name, String as, boolean fromLeftSide) {
		super(name, as);
		this.fromLeftSide = fromLeftSide;
	}
}
