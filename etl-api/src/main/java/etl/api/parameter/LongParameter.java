package etl.api.parameter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LongParameter extends Parameter<Long> {

    public LongParameter(String name, Long value) {
        super(name, value);
    }

    public LongParameter() {
        super();
    }

}
