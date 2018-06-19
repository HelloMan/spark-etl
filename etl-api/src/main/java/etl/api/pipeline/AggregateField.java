package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public class AggregateField extends Field {

    /**
     * the type of aggregate
     */
    private AggregateFunction type;

	@Builder
	AggregateField(@NonNull String name, String as,@NonNull AggregateFunction type) {
		super(name, as);
		this.type = type;
	}

	public static AggregateField max(String name, String as) {
		return AggregateField.builder().name(name).as(as).type(AggregateFunction.MAX).build();
	}
	public static AggregateField max(String name ) {
		return max(name, null);
	}
	public static AggregateField min(String name, String as) {
		return AggregateField.builder().name(name).as(as).type(AggregateFunction.MIN).build();
	}
	public static AggregateField min(String name) {
		return min(name, null);
	}
	public static AggregateField count(String name, String as) {
		return AggregateField.builder().name(name).as(as).type(AggregateFunction.COUNT).build();
	}
	public static AggregateField count(String name ) {
		return count(name, null);
	}
	public static AggregateField avg(String name, String as) {
		return AggregateField.builder().name(name).as(as).type(AggregateFunction.AVG).build();
	}
	public static AggregateField avg(String name ) {
		return avg(name, null);
	}

	public static AggregateField sum(String name, String as) {
		return AggregateField.builder().name(name).as(as).type(AggregateFunction.SUM).build();
	}

	public static AggregateField sum(String name ) {
		return sum(name, null);
	}

}
