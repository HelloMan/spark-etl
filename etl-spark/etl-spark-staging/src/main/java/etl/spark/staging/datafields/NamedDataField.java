package etl.spark.staging.datafields;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(of = {"fieldName"},callSuper = true)
@ToString
public class NamedDataField extends DataField {
    private final String fieldName;

    public NamedDataField(String fieldName, String value) {
        super(value);
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

}