
package etl.api.table;

import com.google.common.collect.ImmutableList;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.math.NumberUtils;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.math.BigDecimal;
import java.util.List;
@Getter
@Setter
@Entity
@DiscriminatorValue(value = "decimal")
public class DecimalField extends Field<BigDecimal> {

    @Column(name = "SCALE")
    private Integer scale;

    @Column(name = "PRECISION")
    private Integer precision;



    @Override
    protected Validation<List<String>,String> doValidate(String value) {
        if (!NumberUtils.isParsable(value)) {
            return Validation.invalid(ImmutableList.of(String.format("Field [%s] expected numeric value - actual: %s", getName().toUpperCase(), value)));
        }
        return Validation.valid(value);
    }


    @Override
    public BigDecimal doParse(String value) {
        return new BigDecimal(value);
    }
}
