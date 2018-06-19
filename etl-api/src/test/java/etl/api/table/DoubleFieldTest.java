package etl.api.table;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class DoubleFieldTest extends AbstractJavaBeanTest<DoubleField> {



    @Test
    public void testGetMaxValue() throws Exception {
        assertThat(new DoubleField().getMaxValue()).isEqualTo(String.valueOf(Double.MAX_VALUE));
    }

    @Test
    public void testGetMinValue() throws Exception {
        assertThat(new DoubleField().getMinValue()).isEqualTo(String.valueOf(Double.MIN_VALUE));
    }

    @Test
    public void testDoParse() throws Exception {
        assertThat(new DoubleField().doParse("10")).isEqualTo(new Double(10));

    }
}