package etl.api.parameter;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Date;

@Getter
@Setter
public class DateParameter extends Parameter<Date> {

	public DateParameter() {
		super();
	}

	public DateParameter(String name, Date value) {
		super(name, value);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
				.append("name", getName())
				.append("value", getValue() == null ? null : getValue().getTime()).build();
	}

}
