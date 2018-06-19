package etl.api.pipeline;

import etl.api.datasource.CsvDataSource;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DataSourceLoaderTest {

    private SourceLoader sourceLoader;
    @Before
    public void setup() {
        sourceLoader = SourceLoader.builder()
                .name("sourceLoader")
                .source(CsvDataSource.builder()
                                .filePath("/a.csv")
                                .build()
                )
                .output(DatasetRefs.datasetRef("sourceLoaderOutput"))
                .build();
    }

    @Test
    public void testAggregator() {
        assertThat(sourceLoader.getName()).isEqualTo("sourceLoader");
        assertThat(sourceLoader.getSource()).isInstanceOf(CsvDataSource.class);

        assertThat(sourceLoader.getOutput().getName()).isEqualTo("sourceLoaderOutput");
    }
}
