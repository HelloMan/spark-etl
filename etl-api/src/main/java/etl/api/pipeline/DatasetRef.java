package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import etl.api.dataset.DatasetMetadata;
import etl.api.parameter.Parameter;
import etl.api.parameter.Parameters;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.Tolerate;
import one.util.streamex.StreamEx;

import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

@Getter
@Builder
@EqualsAndHashCode(of = {"name","filter","parameters"})
@ToString
public class DatasetRef implements Serializable {

    private @NonNull String name;

    @JsonIgnore
    private boolean immediate;

    private String filter;
    @Singular
    private List<String> includes;
    @Singular
    private List<String> excludes;
    @Singular
    private SortedSet<DatasetRefParam> parameters;

    @Tolerate
    public DatasetRef() {
    }

    @JsonIgnore
    public List<Field> getIncludeFields() {
        if (includes == null) {
            return ImmutableList.of();
        }
        return includes.stream().map(Field::of).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Field> getExcludeFields() {
        if (excludes == null) {
            return ImmutableList.of();
        }
        return excludes.stream().map(Field::of).collect(Collectors.toList());
    }


    public DatasetMetadata toDatasetMetadata(Parameters parameters) {
        SortedSet<Parameter> datasetParams = Sets.newTreeSet(StreamEx.of(getParameters())
                .map(dp -> Parameter.create(dp.getDatasetParam(), parameters.getObject(dp.getTransformParam())))
                .toSet());

        return new DatasetMetadata(getName(), datasetParams);
    }

}
