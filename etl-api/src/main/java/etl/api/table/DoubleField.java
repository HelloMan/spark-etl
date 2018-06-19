

package etl.api.table;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

@Getter
@Setter
@Entity
@DiscriminatorValue(value = "double")
public class DoubleField extends NumericField<Double> {


    @Override
    protected String getMaxValue() {
        return String.valueOf(Double.MAX_VALUE);
    }

    @Override
    protected String getMinValue() {
        return String.valueOf(Double.MIN_VALUE);
    }

    @Override
    public Double doParse(String value) {
        return Double.valueOf(value);
    }
}
