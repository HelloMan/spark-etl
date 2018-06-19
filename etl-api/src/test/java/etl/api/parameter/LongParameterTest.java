package etl.api.parameter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LongParameterTest {
	@Test
	public void test() throws Exception {
		Parameter num = Parameter.create("param", 1L);
		assertThat(num.getClass()).isEqualTo(LongParameter.class);
		assertThat(num.toString()).contains("name=param");
	}
}