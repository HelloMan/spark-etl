package etl.api.pipeline;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class UnionTest {
    private Union union;

    @Before
    public void setup() {
        union =  Union.builder()
                .name("union")
                .input(DatasetRefs.datasetRef("employee"))
                .input(DatasetRefs.datasetRef("employee"))
                .output(DatasetRefs.datasetRef("unionOutput"))
                .build();
    }

    @Test
    public void testUnion() {
        assertThat(union.getName()).isEqualTo("union");
        assertThat(union.getInputs().size()).isEqualTo(2);
        assertThat(union.getOutput().getName()).isEqualTo("unionOutput");

    }
}
