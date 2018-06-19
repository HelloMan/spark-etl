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
public class Join extends SingleOutTransform {
	
	private DatasetRef left;
	
	private DatasetRef right;
	
	private List<JoinOn> ons;
	
	private List<JoinField> fields;
	
	private JoinType joinType;
	
	@Builder
	public Join( @NonNull String name,
				@NonNull DatasetRef left,
				@NonNull DatasetRef right,
				DatasetRef output,
				@NonNull @Singular List<JoinOn> ons,
				@NonNull @Singular List<JoinField> fields,
				 JoinType joinType) {
		super(name,output);
		this.left = left;
		this.right = right;
		this.ons = ImmutableList.copyOf(ons);
		this.fields = ImmutableList.copyOf(fields);
		this.joinType = joinType == null ?JoinType.INNER: joinType;
	}
	
	public static JoinBuilder join(DatasetRef left, DatasetRef right, JoinType joinType) {
		return Join.builder().left(left).right(right).joinType(joinType);
	}
	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public List<DatasetRef> getTransformInputs() {
		return ImmutableList.of(left, right);
	}
}