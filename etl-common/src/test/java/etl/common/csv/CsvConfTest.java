package etl.common.csv;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvConfTest {

    @Test
    public void testCsvConf() {
        CsvConf csvConf = CsvConf.builder().delimiter(',').quote("\"").build();

        assertThat(csvConf.getDelimiter()).isEqualTo(',');
        assertThat(csvConf.getQuote()).isEqualTo("\"");

    }
    @Test
    public void testDefault() {
        CsvConf csvConf = CsvConf.getDefault();
        assertThat(csvConf.getDelimiter()).isEqualTo(',');
        assertThat(csvConf.getQuote()).isEqualTo("\"");
    }

}