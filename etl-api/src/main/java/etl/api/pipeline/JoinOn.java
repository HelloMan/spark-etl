package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;

/**
 * Created by jason on 2016/1/24.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public class JoinOn implements Serializable {

	/**
	 * the join  field name of left side
	 *
	 */
	private String left;

	/**
	 * the join  field name of right side
	 *
	 */
	private String right;


	@Builder
	public JoinOn(@NonNull String left, @NonNull  String right) {
		this.left = left;
		this.right = right;
	}

	public static JoinOn on(String left, String right) {
		return JoinOn.builder().left(left).right(right).build();
	}


}
