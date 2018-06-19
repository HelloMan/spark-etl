package etl.api.table;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DateFieldTest extends AbstractJavaBeanTest<DateField> {


    @Test
    public void testDoValidate() throws Exception {
        assertThat(new DateField().doValidate("xx").isInvalid()).isTrue();

        DateField dateField = new DateField();
        dateField.setName("date");
        dateField.setFormat("yyyy-MM-dd");
        assertThat(dateField.doValidate("1982-03-02").isValid()).isTrue();
        assertThat(dateField.doValidate("xx-03-02").isInvalid()).isTrue();
    }
    @Test
    public void testDoParse() throws Exception {
        DateField dateField = new DateField();
        dateField.setName("date");
        dateField.setFormat("yyyy-MM-dd");
        assertThat(dateField.doParse("1982-02-02")).isNotNull();
    }

    @Test
    public void testParseFailed() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> {
                    DateField dateField = new DateField();
                    dateField.setName("date");
                    dateField.setFormat("yyyy-MM-dd");
                    dateField.doParse("1982-02-02-09");
                })
                .withMessage("Failed to parse date: 1982-02-02-09 using format: yyyy-MM-dd");

    }
}