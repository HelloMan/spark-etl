package etl.spark.staging.datafields;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;

@ToString
@EqualsAndHashCode
public abstract class DataField implements Serializable {
    protected final String value;

    public DataField(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}