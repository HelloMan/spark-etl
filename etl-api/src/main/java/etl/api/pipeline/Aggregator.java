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
public class Aggregator extends SingleInOutTransform {
	
	private List<AggregateField> aggregators;

	private List<String> groupBys;

	@Builder
	public Aggregator(@NonNull String name,
					  @NonNull DatasetRef input,
					  DatasetRef output,
					  @NonNull  @Singular List<AggregateField> aggregators,
					  @Singular List<String> groupBys) {
		super(name, input, output);
		this.aggregators = ImmutableList.copyOf(aggregators);
		this.groupBys = ImmutableList.copyOf(groupBys);
	}

	public boolean hasGroupBys() {
		return groupBys != null && !groupBys.isEmpty();
	}

	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}
}