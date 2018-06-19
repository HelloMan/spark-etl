package etl.api.table;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.sql.Timestamp;


@Getter
@Setter
@Entity
@DiscriminatorValue(value = "timestamp")
public class TimestampField extends AbstractDateField<Timestamp> {


    @Override
    public Timestamp doParse(String value) {
        return new Timestamp(dateValue(value,getFormat()).getTime());
    }
}
