

package etl.api.table;

import com.google.common.collect.ImmutableList;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.util.List;

@Getter
@Setter
@Entity
@DiscriminatorValue(value = "boolean")
public class BooleanField extends Field<Boolean> {



    @Override
    protected Validation<List<String>,String> doValidate(String value) {
        if ("true".equalsIgnoreCase(value)
                || "false".equalsIgnoreCase(value)) {
            return Validation.valid(value);
        }
        return Validation.invalid(ImmutableList.of(String.format("Field [%s] expected to be a boolean, - actual: %s", getName().toUpperCase(), value)));
    }

    @Override
    public Boolean doParse(String value) {
        return Boolean.valueOf(value);
    }
}
