package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("squid:UndocumentedApi")
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Map.class, name = "Map"),
        @JsonSubTypes.Type(value = PartitionMap.class, name = "PartitionMap"),
        @JsonSubTypes.Type(value = Join.class, name = "Join"),
        @JsonSubTypes.Type(value = Aggregator.class, name = "Aggregator"),
        @JsonSubTypes.Type(value = Union.class, name = "Union"),
        @JsonSubTypes.Type(value = Distinct.class, name = "Distinct"),
        @JsonSubTypes.Type(value = SourceLoader.class, name = "SourceLoader"),
        @JsonSubTypes.Type(value = Split.class, name = "Split")
})
@Getter
@NoArgsConstructor
@EqualsAndHashCode
public abstract class Transform implements Serializable {

    private String name;

    public Transform(String name) {
        this.name = name;
    }

    @JsonIgnore
    public abstract boolean isSingleOutput();


    public abstract void accept(TransformVisitor visitor);

    @JsonIgnore
    abstract public List<DatasetRef> getTransformInputs();
    @JsonIgnore
    abstract public List<DatasetRef> getTransformOutputs();


}