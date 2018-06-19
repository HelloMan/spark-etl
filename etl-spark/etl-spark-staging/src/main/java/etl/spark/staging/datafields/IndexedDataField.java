package etl.spark.staging.datafields;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true,of = {"index"})
public class IndexedDataField extends DataField {
    private Integer index;

    public IndexedDataField(int index, String value) {
        super(value);
        this.index = index;
    }

    public Integer getIndex() {
        return index;
    }
}
