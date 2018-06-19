package etl.api.pipeline;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class DistinctTest {

    private Distinct distinct;

    @Before
    public void setup() {
        distinct =  Distinct.builder().name("distinct")
                .field("name").input(DatasetRefs.datasetRef("employee"))
                .output(DatasetRefs.datasetRef("distinctOutput"))
                .build();
    }

    @Test
    public void testDistinct() {
        assertThat(distinct.getName()).isEqualTo("distinct");
        assertThat(distinct.getInput().getName()).isEqualTo("employee");
        assertThat(distinct.getFields().size()).isEqualTo(1);
        assertThat(distinct.getOutput().getName()).isEqualTo("distinctOutput");

    }
}
