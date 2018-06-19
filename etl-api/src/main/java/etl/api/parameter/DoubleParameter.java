package etl.api.parameter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DoubleParameter extends Parameter<Double> {

    public DoubleParameter() {
        super();
    }

    public DoubleParameter(String name, Double value) {
        super(name, value);
    }

}
