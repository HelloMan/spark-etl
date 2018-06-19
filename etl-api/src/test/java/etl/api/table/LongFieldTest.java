package etl.api.table;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LongFieldTest extends AbstractJavaBeanTest<LongField> {


    @Test
    public void testGetMaxValue() throws Exception {
        assertThat(new LongField().getMaxValue()).isEqualTo(String.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testGetMinValue() throws Exception {
        assertThat(new LongField().getMinValue()).isEqualTo(String.valueOf(Long.MIN_VALUE));
    }

    @Test
    public void testDoParse() throws Exception {
        assertThat(new LongField().doParse("10")).isEqualTo(new Long(10));

    }


}