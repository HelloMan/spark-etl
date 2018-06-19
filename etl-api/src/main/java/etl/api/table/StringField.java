
package etl.api.table;

import com.google.common.collect.ImmutableList;
import etl.common.lang.CachedPattern;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.util.List;

@Getter
@Setter
@Entity
@DiscriminatorValue(value = "string")
public class StringField extends Field<String> {

    @Column(name = "MAX_LENGTH")
    private Integer maxLength;
    @Column(name = "MIN_LENGTH")
    private Integer minLength;
    @Column(name = "REGULAR_EXPRESSION")
    private String regularExpression;

    @Override
    protected Validation<List<String>,String> doValidate(String value) {
        if (getMaxLength() != null && value.length()> getMaxLength()) {
            return Validation.invalid(
                    ImmutableList.of(String.format("Field [%s] expected to have a maximum length of %d - actual: %d",
                    getName().toUpperCase(), getMaxLength(), value.length()))
            );

        }

        if (getMinLength() != null && value.length()< getMinLength()) {
            return Validation.invalid(
                    ImmutableList.of(String.format("Field [%s] expected to have a minimum length of %d - actual: %d",
                    getName().toUpperCase(), getMinLength(), value.length()))
            );
        }

        if (StringUtils.isNotEmpty(getRegularExpression())
                && !CachedPattern.compile(getRegularExpression()).matches(value)) {
            return Validation.invalid(
                    ImmutableList.of(String.format("Field [%s] expected to match the regex %s - actual: %s",
                    getName().toUpperCase(), getRegularExpression(), value))
            );
        }
        return Validation.valid(value);
    }

    @Override
    public String doParse(String value) {
        return value;
    }
}
