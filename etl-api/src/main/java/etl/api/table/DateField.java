package etl.api.table;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.sql.Date;

@Getter
@Setter
@Entity
@DiscriminatorValue(value = "date")
public class DateField extends AbstractDateField<Date> {



    @Override
    public java.sql.Date doParse(String value) {
        return new java.sql.Date(dateValue(value,getFormat()).getTime());
    }

}
