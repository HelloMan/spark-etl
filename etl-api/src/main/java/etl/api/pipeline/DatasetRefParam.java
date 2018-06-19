package etl.api.pipeline;

import com.google.common.collect.ComparisonChain;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class DatasetRefParam implements Serializable, Comparable<DatasetRefParam> {

    private @NonNull String datasetParam;
    private  @NonNull String transformParam;

    public DatasetRefParam(String datasetParam, String transformParam) {
        this.datasetParam = datasetParam;
        this.transformParam = transformParam;
    }

	@Override
	public int compareTo(DatasetRefParam that) {
		return ComparisonChain.start()
				.compare(datasetParam, that.datasetParam)
				.compare(transformParam, that.transformParam)
				.result();
	}
}