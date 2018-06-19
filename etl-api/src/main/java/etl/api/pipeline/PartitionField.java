package etl.api.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;


@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Getter
public class PartitionField extends Field {
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<OrderBy> orderBys;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<String> partitionBys;

    private PartitionFunction function;

    @Builder
    PartitionField(@NonNull String name,
                   String as,
                   @Singular List<OrderBy> orderBys,
                   PartitionFunction function,
                   @Singular List<String> partitionBys) {
        super(name, as);
        this.orderBys = ImmutableList.copyOf(orderBys);
        this.function = function;
        this.partitionBys = ImmutableList.copyOf(partitionBys);
    }

    public static PartitionFieldBuilder max(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.MAX);
        return builder;
    }

    public static PartitionFieldBuilder max(String field, String as) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.MAX).as(as);
        return builder;
    }

    public static PartitionFieldBuilder min(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.MIN);
        return builder;
    }

    public static PartitionFieldBuilder sum(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.SUM);
        return builder;
    }

    public static PartitionFieldBuilder denseRank() {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.function(PartitionFunction.DENSE_RANK).name("denseRank").as("denseRank");
        return builder;
    }

    public static PartitionFieldBuilder first(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.FIRST);
        return builder;
    }

    public static PartitionFieldBuilder last(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.LAST);
        return builder;
    }

    public static PartitionFieldBuilder avg(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.AVG);
        return builder;
    }

    public static PartitionFieldBuilder count(String field) {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.name(field).function(PartitionFunction.COUNT);
        return builder;
    }

    public static PartitionFieldBuilder rank() {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.function(PartitionFunction.RANK).name("rank").as("rank");
        return builder;
    }

    public static PartitionFieldBuilder rowNumber() {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.function(PartitionFunction.ROW_NUMBER).name("rowNumber").as("rowNumber");
        return builder;
    }

    public static PartitionFieldBuilder percentRank() {
        PartitionFieldBuilder builder = PartitionField.builder();
        builder.function(PartitionFunction.PERCENT_RANK).name("percentRank").as("percentRank");
        return builder;
    }
}
