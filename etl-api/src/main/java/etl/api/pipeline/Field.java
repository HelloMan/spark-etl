package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@NoArgsConstructor
public class Field implements Serializable {

	private static final String ALL_FIELD = "*";
	/**
	 * field original name in dataset
	 */
	protected String name;
	
	/**
	 * field alias
	 */
	protected String as;

	@Builder(builderMethodName = "newBuilder")
	public Field( String name, String as) {

		this.name = name;
		this.as = as;
	}

	@JsonIgnore
	public String getAlias() {
		return StringUtils.isNotEmpty(as) ? as : name;
	}

	public static Field all() {
		return Field.newBuilder().name(ALL_FIELD).build();
	}
	@JsonIgnore
	public boolean isAll() {
		return ALL_FIELD.equals(name);
	}

	public static Field of(String field) {
		return Field.newBuilder().name(field).as(field).build();
	}
	
	public static Field of(String field, String as) {
		return Field.newBuilder().name(field).as(as).build();
	}

}