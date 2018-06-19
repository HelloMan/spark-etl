package etl.api.pipeline;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.Serializable;
import java.text.ParseException;

@Getter
@ToString
public class FieldType implements Serializable {

	public static final FieldType STRING = FieldType.of(Type.STRING);
	public static final FieldType INT = FieldType.of(Type.INT);
	public static final FieldType LONG = FieldType.of(Type.LONG);
	public static final FieldType FLOAT = FieldType.of(Type.FLOAT);
	public static final FieldType DOUBLE = FieldType.of(Type.DOUBLE);
	public static final FieldType TIMESTAMP = FieldType.of(Type.TIMESTAMP);
	public static final FieldType BOOLEAN = FieldType.of(Type.BOOLEAN);
	public static final FieldType NULL = FieldType.of(Type.NULL);

	public static FieldType date(String format) {
		return FieldType.of(Type.DATE, format);
	}

	public enum Type {
		INT {
			@Override
			Object parse(String value, FieldType fieldType) {
				return Integer.valueOf(value);
			}
		},
		LONG {
			@Override
			Object parse(String value, FieldType fieldType) {
				return Long.valueOf(value);
			}
		},
		FLOAT {
			@Override
			Object parse(String value, FieldType fieldType) {
				return Float.valueOf(value);
			}
		},
		DOUBLE {
			@Override
			Object parse(String value, FieldType fieldType) {
				return Double.valueOf(value);
			}
		},
		DATE {
			@Override
			Object parse(String value, FieldType fieldType) {
				try {
					return DateUtils.parseDate(value, fieldType.getFormat());
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
		},
		TIMESTAMP {
			@Override
			Object parse(String value, FieldType fieldType) {
				try {
					return DateUtils.parseDate(value, fieldType.getFormat());
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
		},
		STRING {
			@Override
			Object parse(String value, FieldType fieldType) {
				return value;
			}
		},
		BOOLEAN {
			@Override
			Object parse(String value, FieldType fieldType) {
				return Boolean.valueOf(value);
			}
		},
		NULL {
			@Override
			Object parse(String value, FieldType fieldType) {
				return null;
			}
		};

		abstract Object parse(String value, FieldType fieldType);

	}

	private Type type;
	private String format;

	public static FieldType of(Type datasetFieldType) {
		FieldType type = new FieldType();
		type.type = datasetFieldType;
		return type;
	}

	public static FieldType of(Type datasetFieldType, String format) {
		FieldType type = new FieldType();
		type.type = datasetFieldType;
		type.format = format;
		return type;
	}

	public Object parse(Object value) {
		if (value == null) {
			return null;
		}

		if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
			return value;
		}

		return type.parse(value.toString(), this);
	}

}