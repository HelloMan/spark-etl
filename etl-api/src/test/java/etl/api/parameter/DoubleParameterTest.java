package etl.api.parameter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleParameterTest {
	@Test
	public void test() throws Exception {
		Parameter num = Parameter.create("param", 1.0D);
		assertThat(num.getClass()).isEqualTo(DoubleParameter.class);
		assertThat(num.toString()).contains("name=param");
	}
}