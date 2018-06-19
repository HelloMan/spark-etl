package etl.api.pipeline;

public interface TransformVisitor {

    void visit(Map map);

    void visit(Distinct distinct);

    void visit(Aggregator aggregator);

    void visit(PartitionMap partitionMap);

    void visit(Join join);

    void visit(Union union);

    void visit(Split split);

    void visit(SourceLoader sourceLoader);

}
