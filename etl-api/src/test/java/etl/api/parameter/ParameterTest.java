package etl.api.parameter;

import org.junit.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

public class ParameterTest {
	@Test(expected = IllegalArgumentException.class)
    public void test() throws Exception {
		Parameter.create("param", BigDecimal.valueOf(123));
	}
}