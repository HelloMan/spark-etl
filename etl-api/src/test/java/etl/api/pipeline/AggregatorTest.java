package etl.api.pipeline;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class AggregatorTest {
    private Aggregator aggregator;
    @Before
    public void setup() {
        aggregator = Aggregator.builder()
                .name("aggregator")
                .aggregator(AggregateField.avg("age"))
                .input(DatasetRefs.datasetRef("mapOutput"))
                .output(DatasetRefs.datasetRef("aggregatorOutput"))
                .build();
    }

    @Test
    public void testAggregator() {
        assertThat(aggregator.getName()).isEqualTo("aggregator");
        assertThat(aggregator.getGroupBys()).isNullOrEmpty();
        assertThat(aggregator.getAggregators().size()).isEqualTo(1);
        AggregateField aggregateField = aggregator.getAggregators().iterator().next();

        assertThat(aggregateField.getType()).isEqualTo(AggregateFunction.AVG);
        assertThat(aggregateField.getName()).isEqualTo("age");

    }

}
