package etl.api.parameter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IntParameter extends Parameter<Integer> {

    public IntParameter(String name, Integer value) {
        super(name, value);
    }

    public IntParameter() {
        super();
    }

}
