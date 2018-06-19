package etl.api.parameter;

import lombok.Getter;

@Getter
public class BooleanParameter extends Parameter<Boolean> {

    public BooleanParameter(String name, Boolean value) {
        super(name, value);
    }

    public BooleanParameter() {
        super();
    }

}
