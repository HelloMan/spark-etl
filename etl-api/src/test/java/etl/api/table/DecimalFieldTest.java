package etl.api.table;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

public class DecimalFieldTest extends AbstractJavaBeanTest<DecimalField> {



    @Test
    public void testDoValidate() throws Exception {
        DecimalField result = new DecimalField();
        result.setPrecision(2);
        result.setName("decimal");
        result.setScale(2);
        assertThat(result.doValidate("2").isValid()).isTrue();
        assertThat(result.doValidate("x2").isInvalid()).isTrue();
    }

    @Test
    public void testDoParse() throws Exception {
        DecimalField result = new DecimalField();
        result.setPrecision(2);
        result.setScale(2);
        assertThat(result.doParse("2")).isEqualTo(new BigDecimal(2));
    }
}