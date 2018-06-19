package etl.api.parameter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StringParameter extends Parameter<String> {
    public StringParameter() {
        super();
    }

    public StringParameter(String name, String value) {
        super(name, value);
    }

}
