package etl.api.pipeline;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetRefsTest {

    @Test
    public void testFromTransform() throws Exception {
        Split split = Split.builder()
                .name("split")
                .condition("a>1")
                .input(DatasetRef.builder().name("input").build())
                .output1(DatasetRef.builder().name("output1").build())
                .output2(DatasetRef.builder().name("output2").build())
                .build();
        assertThat(DatasetRefs.fromTransform(split, true).getName()).isEqualTo("output1");
        assertThat(DatasetRefs.fromTransform(split, false).getName()).isEqualTo("output2");
    }

    @Test
    public void testFromTransform1() throws Exception {
        Map map = Map.builder()
                .name("map")
                .input(DatasetRef.builder().name("input").build())
                .output(DatasetRef.builder().name("output").build())
                .build();
        assertThat(DatasetRefs.fromTransform(map).getName()).isEqualTo("output");
        assertThat(DatasetRefs.fromTransform(map).getParameters().size()).isEqualTo(0);
    }

    @Test
    public void testFromTransform2() throws Exception {
        Map map = Map.builder()
                .name("map")
                .input(DatasetRef.builder().name("input").build())
                .output(DatasetRef.builder().name("output").build())
                .build();
        assertThat(DatasetRefs.fromTransform(map).getName()).isEqualTo("output");
        assertThat(DatasetRefs.fromTransform(map).getParameters().size()).isEqualTo(0);

        assertThat(DatasetRefs.fromTransform(map, "a>1").getFilter()).isEqualTo("a>1");
    }

    @Test
    public void testDatasetRef() throws Exception {
        assertThat(DatasetRefs.datasetRef("name", "filter").getName()).isEqualTo("name");
        assertThat(DatasetRefs.datasetRef("name", "filter").getFilter()).isEqualTo("filter");
    }

    @Test
    public void testDatasetRef1() throws Exception {
        assertThat(DatasetRefs.datasetRef("name" ).getName()).isEqualTo("name");
    }

    @Test
    public void testDatasetRef2() throws Exception {
        assertThat(DatasetRefs.datasetRef("name", ImmutableSet.of(new DatasetRefParam("a", "b"))).getParameters().size()).isEqualTo(1);
    }

    @Test
    public void testDatasetRef3() throws Exception {
        assertThat(DatasetRefs.datasetRef("name", ImmutableSet.of(new DatasetRefParam("a", "b")),"filter").getParameters().size()).isEqualTo(1);
        assertThat(DatasetRefs.datasetRef("name", ImmutableSet.of(new DatasetRefParam("a", "b")), "filter").getFilter() ).isEqualTo("filter");
    }
}