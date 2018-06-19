package etl.api.parameter;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ParametersTest {
	
	private static final String PARAM = "param";
	private Parameters emptyParams;
	private Parameters filledParams;

	@Before
	public void init() {
		emptyParams = new Parameters(Maps.newHashMap());

		filledParams = new Parameters();
		filledParams.setInt("intParam", 111);
		filledParams.setLong("longParam", 222L);
		filledParams.setString("stringParam", "123");
	}

	@Test
	public void unmatchedBoolean() {
		assertThat(emptyParams.getBoolean(PARAM)).isFalse();
	}

	@Test
	public void unmatchedLong() {
		assertThat(emptyParams.getLong(PARAM)).isEqualTo(0L);
	}

	@Test
	public void unmatchedInt() {
		assertThat(emptyParams.getInt(PARAM)).isEqualTo(0);
	}

	@Test
	public void unmatchedDouble() {
		assertThat(emptyParams.getDouble(PARAM)).isEqualTo(0.0);
	}

	@Test
	public void unmatchedDate() {
		assertThat(emptyParams.getDate(PARAM)).isNull();
	}

	@Test
	public void unmatchedObject() {
		assertThat(emptyParams.getObject(PARAM)).isNull();
	}

	@Test
	public void isEmpty() {
		assertThat(emptyParams.isEmpty()).isTrue();
	}

	@Test
	public void matchedInt() {
		assertThat(filledParams.getInt("intParam", 1)).isEqualTo(111);
	}

	@Test
	public void matchedLong() {
		assertThat(filledParams.getLong("longParam", 1L)).isEqualTo(222L);
	}

	@Test
	public void matchedString() {
		assertThat(filledParams.getString("stringParam", "333")).isEqualTo("123");
	}
}
