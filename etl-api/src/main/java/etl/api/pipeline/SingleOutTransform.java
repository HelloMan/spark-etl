package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
public abstract class SingleOutTransform extends Transform {

    private DatasetRef output;

    public SingleOutTransform(  String name, DatasetRef output) {
        super(name);
        this.output = output;
    }



    @Override
    @JsonIgnore
    public boolean isSingleOutput() {
        return true;
    }

    @Override
    @JsonIgnore
    public List<DatasetRef> getTransformOutputs() {
        return output == null ? ImmutableList.of() : ImmutableList.of(output);
    }
}
