package etl.api.pipeline;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class PartitionMapTest {
    private PartitionMap partitionMap;

    @Before
    public void setup() {
        partitionMap =  PartitionMap.builder()
                .name("partitionMap")
                .input(DatasetRefs.datasetRef("employee"))
                .output(DatasetRefs.datasetRef("partitionMapOutput"))
                .field(PartitionField.avg("age")
                        .partitionBy("sex").as("avgAge")
                        .orderBy(new OrderBy("name",true))
                        .build())

                .build();
    }

    @Test
    public void testPartitionMap() {
        assertThat(partitionMap.getName()).isEqualTo("partitionMap");
        assertThat(partitionMap.getInput().getName()).isEqualTo("employee");
        assertThat(partitionMap.getOutput().getName()).isEqualTo("partitionMapOutput");
        assertThat(partitionMap.getFields().size()).isEqualTo(1);
        PartitionField partitionField = partitionMap.getFields().iterator().next();
        assertThat(partitionField.getFunction()).isEqualTo(PartitionFunction.AVG);
        assertThat(partitionField.getPartitionBys().size()).isEqualTo(1);
        assertThat(partitionField.getOrderBys().size()).isEqualTo(1);

    }
}
