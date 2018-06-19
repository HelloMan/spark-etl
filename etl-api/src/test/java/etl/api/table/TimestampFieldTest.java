package etl.api.table;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampFieldTest {


    @Test
    public void testDoParse() throws Exception {
        TimestampField dateField = new TimestampField();
        dateField.setFormat("hh:mm:ss");
        assertThat(dateField.doParse("12:20:22")).isNotNull();
    }
}