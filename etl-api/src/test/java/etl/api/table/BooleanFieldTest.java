package etl.api.table;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class BooleanFieldTest extends AbstractJavaBeanTest<BooleanField> {


    @Test
    public void testDoValidate() throws Exception {
        BooleanField booleanField = new BooleanField();
        booleanField.setName("boolean");

        assertThat(booleanField.doValidate("true").isValid()).isTrue();
        assertThat(booleanField.doValidate("false").isValid()).isTrue();
        assertThat(booleanField.doValidate("xx").isInvalid()).isTrue();

    }
    @Test
    public void testDoParse() throws Exception {
        assertThat(new BooleanField().doParse("true")).isTrue();
        assertThat(new BooleanField().doParse("false")).isFalse();
    }
}