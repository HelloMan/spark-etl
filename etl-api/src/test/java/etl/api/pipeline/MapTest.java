package etl.api.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class MapTest {


    @Test
    public  void testMap() {
        Map map = Map.builder()
                .name("map")
                .field(MapField.builder().name("age")
                        .fieldType(FieldType.of(FieldType.Type.INT))
                        .build())
                .input(DatasetRefs.datasetRef("employee"))
                .output(DatasetRefs.datasetRef("mapOutput"))
                .build();

        assertThat(map.getName()).isEqualTo("map");
        assertThat(map.getFields().size()).isEqualTo(1);
        assertThat(map.getFields().iterator().next().getFieldType().getType()).isEqualTo(FieldType.Type.INT);
        assertThat(map.getInput().getName()).isEqualTo("employee");
        assertThat(map.getOutput().getName()).isEqualTo("mapOutput");



    }

}