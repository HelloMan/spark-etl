package etl.api.table;

import com.google.common.collect.ImmutableList;
import javaslang.control.Validation;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.util.List;


public abstract class NumericField<T extends Comparable> extends Field<T> {


    protected Validation<List<String>,String> checkRange(String value, String min, String max) {
        BigDecimal curValue = new BigDecimal(value);
        BigDecimal maxValue = new BigDecimal(max);
        BigDecimal minValue = new BigDecimal(min);
        if (curValue.compareTo(minValue) < 0 || curValue.compareTo(maxValue) > 0) {
            return Validation.invalid(
                    ImmutableList.of(String.format("Field [%s] expected to be a value between %s and %s - actual: %s",
                    getName().toUpperCase(), min, max, value))
            );
        }
        return Validation.valid(value);
    }

    @Override
    protected Validation<List<String>,String> doValidate(String value) {
        if (!NumberUtils.isCreatable(value)) {
            return Validation.invalid(ImmutableList.of(String.format("Field [%s] expected numeric value - actual: %s", getName().toUpperCase(), value)));
        }

        return checkRange(value, getMinValue(), getMaxValue());

    }

    protected abstract String getMaxValue();

    protected abstract String getMinValue();



}
