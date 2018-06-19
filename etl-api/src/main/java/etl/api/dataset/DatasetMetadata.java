package etl.api.dataset;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import etl.api.parameter.Parameter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collection;
import java.util.Objects;
import java.util.SortedSet;

@NoArgsConstructor
@Getter
public class DatasetMetadata  {

    private String name;

    private SortedSet<Parameter> metadata;


    public DatasetMetadata(String name, Collection<Parameter> metadata) {
        Objects.requireNonNull(name);
        this.name = name;
        this.metadata = Sets.newTreeSet(metadata);
    }

    public DatasetMetadata(String name) {
		Objects.requireNonNull(name);
		this.name = name;
		this.metadata = null;
	}

    public static DatasetMetadata of(String table) {
        return new DatasetMetadata(table);
    }

    public String toMetadataKey() {
        return new DefaultDatasetMetadataKeyGenerator().generateKey(metadata);
    }

	@Override
	public String toString() {
		ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
				.append("name", this.name);
		if (CollectionUtils.isNotEmpty(metadata)) {
			builder.append("metadata", Joiner.on(";").join(metadata));
		}
		return builder.build();
	}
}
