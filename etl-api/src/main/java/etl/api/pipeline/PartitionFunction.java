package etl.api.pipeline;


public enum PartitionFunction {
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX,
    DENSE_RANK,
    RANK,
    ROW_NUMBER,
    PERCENT_RANK,
    FIRST,
    LAST
}
