package etl.api.table;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class StringFieldTest {


    @Test
    public void testDoValidate() throws Exception {
        StringField stringField = new StringField();
        stringField.setName("string");
        stringField.setMaxLength(9);
        stringField.setMinLength(5);
        stringField.setRegularExpression("abc");

        assertThat(stringField.doValidate("abc").isValid()).isFalse();
        assertThat(stringField.doValidate("1234567890").isValid()).isFalse();
        assertThat(stringField.doValidate("abcdef").isValid()).isFalse();
    }

    @Test
    public void testDoParse() throws Exception {
        StringField stringField = new StringField();
        assertThat(stringField.doParse("sss")).isEqualTo("sss");
    }
}