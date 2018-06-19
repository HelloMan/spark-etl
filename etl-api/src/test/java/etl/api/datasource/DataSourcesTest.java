package etl.api.datasource;


import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class DataSourcesTest {
    @Test
    public void testCsv() {
        assertThat(DataSources.csv("/").getFilePath()).isEqualTo("/");
    }
}
