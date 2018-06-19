package etl.api.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitTest {

    private Split createSplit() {
        return Split.builder()
                .name("split")
                .condition("age>40")
                .input(DatasetRefs.datasetRef("employee"))
                .output1(DatasetRefs.datasetRef("oldEmployee"))
                .output2(DatasetRefs.datasetRef("youngEmployee"))
                .build();
    }


    @Test
    public  void testMap() {
        //act
        Split split = createSplit();

        //assert
        assertThat(split.getName()).isEqualTo("split");
        assertThat(split.getCondition()).isEqualTo("age>40");
        assertThat(split.getInput().getName()).isEqualTo("employee");
        assertThat(split.getOutput1().getName()).isEqualTo("oldEmployee");
        assertThat(split.getOutput2().getName()).isEqualTo("youngEmployee");
    }

}
