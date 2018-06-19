package etl.api.parameter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntParameterTest {
	@Test
	public void test() throws Exception {
		Parameter num = Parameter.create("param", 1);
		assertThat(num.getClass()).isEqualTo(IntParameter.class);
		assertThat(num.toString()).contains("name=param");
	}
}