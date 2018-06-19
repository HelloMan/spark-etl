package etl.api.table;

import com.google.common.collect.ImmutableList;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.text.ParseException;
import java.util.Date;
import java.util.List;


@Getter
@Setter
@MappedSuperclass
@Slf4j
public abstract class AbstractDateField<T extends Comparable> extends Field<T> {

    @Column(name = "FORMAT")
    private String format;

    @Override
    protected Validation<List<String>,String> doValidate(String value) {
        if (value == null || format == null) {
            return Validation.invalid(ImmutableList.of("No format or value specified"));
        }
        try {
            DateUtils.parseDate(value, format);
            return Validation.valid(value);
        } catch (ParseException e) {
            String errorMessage = String.format("Field [%s] expected to be a date with format %s - actual: %s",
                    getName().toUpperCase(), format, value);
            log.error(errorMessage, e);
            return Validation.invalid(ImmutableList.of(errorMessage));
        }

    }

    protected Date dateValue(String value, String format) {
        try {
            return DateUtils.parseDate(value, format);
        } catch (ParseException e) {
            // due to data is validated, this branch will never be reached.
            String msg = String.format("Failed to parse date: %s using format: %s", value, format);
            throw new IllegalArgumentException(msg, e);
        }
    }




}
