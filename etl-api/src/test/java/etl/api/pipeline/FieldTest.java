package etl.api.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldTest {

	@Test
	public void basic_field() {
		Field field1 = Field.of("age", "ageField");
		assertThat(field1.getAlias()).isEqualTo("ageField");

		Field field2 = Field.of("age");
		assertThat(field2.getAlias()).isEqualTo("age");

		Field field3 = Field.all();
		assertThat(field3.getName()).isEqualTo("*");
		assertThat(field3.isAll()).isTrue();
	}

	@Test
	public void aggregate_field() {
		AggregateField field1 = AggregateField.count("*");
		assertThat(field1.getType()).isEqualTo(AggregateFunction.COUNT);

		AggregateField field2 = AggregateField.max("Age");
		assertThat(field2.getType()).isEqualTo(AggregateFunction.MAX);

		AggregateField field3 = AggregateField.min("Age");
		assertThat(field3.getType()).isEqualTo(AggregateFunction.MIN);

		AggregateField field4 = AggregateField.sum("Age");
		assertThat(field4.getType()).isEqualTo(AggregateFunction.SUM);
	}
}
