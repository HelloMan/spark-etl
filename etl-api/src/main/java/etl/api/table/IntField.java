

package etl.api.table;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Getter
@Setter
@Entity
@DiscriminatorValue(value = "integer")
public class IntField extends NumericField<Integer> {


    @Override
    public Integer doParse(String value) {
        return Integer.valueOf(value);
    }

    @Override
    protected String getMaxValue() {
        return String.valueOf(Integer.MAX_VALUE);
    }

    @Override
    protected String getMinValue() {
        return String.valueOf(Integer.MIN_VALUE);
    }
}
