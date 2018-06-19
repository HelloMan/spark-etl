package etl.api.parameter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Date;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = StringParameter.class, name = "string"),
        @JsonSubTypes.Type(value = IntParameter.class, name = "int"),
        @JsonSubTypes.Type(value = LongParameter.class, name = "long"),
        @JsonSubTypes.Type(value = BooleanParameter.class, name = "boolean"),
        @JsonSubTypes.Type(value = DateParameter.class, name = "date"),
        @JsonSubTypes.Type(value = DoubleParameter.class, name = "double")
})
@EqualsAndHashCode(of = {"name"})
@AllArgsConstructor
@NoArgsConstructor
public abstract class Parameter<T> implements Comparable<Parameter<T>> {

    private String name;

    private T value;

    @Override
    public final int compareTo(Parameter<T> other) {
        return ComparisonChain.start()
                .compareFalseFirst(this.getName() == null, other.getName() == null)
                .compare(this.getName(), other.getName(), Ordering.natural().nullsLast())
                .result();
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("name", getName())
                .append("value", getValue()).build();

    }

	public static Parameter create(String name, Object value) {
		Preconditions.checkArgument(StringUtils.isNoneBlank(name), "dataset parameter name cannot be empty");
		Preconditions.checkArgument(value != null && StringUtils.isNoneBlank(value.toString()), "dataset parameter value cannot be empty");

		Class<?> valueClass = value.getClass();

		if (valueClass == String.class) {
			return new StringParameter(name, value.toString());
		} else if (valueClass == Integer.class) {
			return new IntParameter(name, (Integer) value);
		} else if (valueClass == Double.class) {
			return new DoubleParameter(name, (Double) value);
		} else if (valueClass == Long.class) {
			return new LongParameter(name, (Long) value);
		} else if (valueClass == Boolean.class) {
			return new BooleanParameter(name, (Boolean) value);
		} else if (valueClass == Date.class) {
			return new DateParameter(name, (Date) value);
		} else {
			throw new IllegalArgumentException("Unsupported dataset parameter type: " + valueClass);
		}
	}
}