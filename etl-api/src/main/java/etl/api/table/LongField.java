package etl.api.table;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;


@Getter
@Setter
@Entity
@DiscriminatorValue(value = "long")
public class LongField extends NumericField<Long> {



    @Override
    public Long doParse(String value) {
        return Long.valueOf(value);
    }

    @Override
    protected String getMaxValue() {
        return String.valueOf(Long.MAX_VALUE);
    }

    @Override
    protected String getMinValue() {
        return String.valueOf(Long.MIN_VALUE);
    }

}
