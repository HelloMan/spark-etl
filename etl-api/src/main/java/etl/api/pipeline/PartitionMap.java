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
@NoArgsConstructor
@Getter
public class PartitionMap extends SingleInOutTransform {
	
	private List<PartitionField> fields;
	
	@Builder
	public PartitionMap( @NonNull  String name,
						@NonNull DatasetRef input,
						DatasetRef output,
						@NonNull @Singular List<PartitionField> fields) {
		super(name,input,output);
		this.fields = ImmutableList.copyOf(fields);
	}
	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}
}