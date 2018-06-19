package etl.api.parameter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StringParameterTest {
	@Test
	public void test() throws Exception {
		Parameter date = Parameter.create("param", "abc");
		assertThat(date.getClass()).isEqualTo(StringParameter.class);
	}
}