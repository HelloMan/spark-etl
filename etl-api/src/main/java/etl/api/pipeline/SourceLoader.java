package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import etl.api.datasource.DataSource;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Getter
public class SourceLoader extends SingleOutTransform {
	private DataSource source;


	@Builder
	public SourceLoader(@NonNull  String name, @NonNull DataSource source, DatasetRef output) {
		super(name,output);

		this.source = source;
	}
	@Override
	public void accept(TransformVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public List<DatasetRef> getTransformInputs() {
		return ImmutableList.of();
	}
}