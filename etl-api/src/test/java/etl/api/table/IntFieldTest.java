package etl.api.table;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntFieldTest {



    @Test
    public void testGetMaxValue() throws Exception {
        assertThat(new IntField().getMaxValue()).isEqualTo(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void testGetMinValue() throws Exception {
        assertThat(new IntField().getMinValue()).isEqualTo(String.valueOf(Integer.MIN_VALUE));
    }

    @Test
    public void testDoParse() throws Exception {
        assertThat(new IntField().doParse("10")).isEqualTo(new Integer(10));

        assertThat(new IntField().parse("")).isNull();
        assertThat(new IntField().parse("10")).isEqualTo(new Integer(10));

    }

    @Test
    public void testCheckRange() throws Exception {
        IntField intField = new IntField();
        intField.setName("int");
        assertThat(intField.validate("ss").isInvalid()).isTrue();
        assertThat(intField.validate(String.valueOf(Long.MAX_VALUE)).isInvalid()).isTrue();
        assertThat(intField.validate("22").isValid()).isTrue();
    }

    @Test
    public void testValidate() {
        IntField intField = new IntField();
        intField.setName("int");

        assertThat(intField.validate("").isValid()).isTrue();

        intField.setKey(true);
        assertThat(intField.validate("").isValid()).isFalse();

        intField.setKey(false);
        intField.setNullable(false);
        assertThat(intField.validate("").isValid()).isFalse();
    }

}