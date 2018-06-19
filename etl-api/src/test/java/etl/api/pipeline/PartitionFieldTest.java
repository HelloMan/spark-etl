package etl.api.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionFieldTest {

    @Test
    public void testMax() throws Exception {
        PartitionField field =  PartitionField.max("age").as("maxAge").build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.MAX);
    }

    @Test
    public void testMin() throws Exception {
        PartitionField field =  PartitionField.min("age").as("minAge").build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.MIN);
    }

    @Test
    public void testSum() throws Exception {
        PartitionField field =  PartitionField.sum("age").as("sumAge").build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.SUM);
    }

    @Test
    public void testDenseRank() throws Exception {
        PartitionField field =  PartitionField.denseRank().name("denseRank").as("denseRank").build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.DENSE_RANK);
    }

    @Test
    public void testFirst() throws Exception {
        PartitionField field =  PartitionField.first("age").as("firstRow") .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.FIRST);
    }

    @Test
    public void testLast() throws Exception {
        PartitionField field =  PartitionField.last("age").as("lastRow") .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.LAST);
    }

    @Test
    public void testAvg() throws Exception {
        PartitionField field =  PartitionField.avg("age").as("avgAge") .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.AVG);
    }

    @Test
    public void testCount() throws Exception {
        PartitionField field =  PartitionField.count("*").as("count") .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.COUNT);
    }

    @Test
    public void testRank() throws Exception {
        PartitionField field =  PartitionField.rank().as("rank") .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.RANK);
    }

    @Test
    public void testRowNumber() throws Exception {
        PartitionField field =  PartitionField.rowNumber()  .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.ROW_NUMBER);
    }

    @Test
    public void testPercentRank() throws Exception {
        PartitionField field =  PartitionField.percentRank()  .build();
        assertThat(field.getFunction()).isEqualTo(PartitionFunction.PERCENT_RANK);
    }
}