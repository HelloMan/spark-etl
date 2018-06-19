package etl.api.parameter;

import org.junit.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class DateParameterTest {
	@Test
    public void test() throws Exception {
		Parameter<Date> date = Parameter.create("param", new Date());
		assertThat(date.getClass()).isEqualTo(DateParameter.class);
		assertThat(date.toString()).contains("name=param");
	}
}