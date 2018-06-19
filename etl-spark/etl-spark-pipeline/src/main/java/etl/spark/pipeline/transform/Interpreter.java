package etl.spark.pipeline.transform;

import etl.api.pipeline.Aggregator;
import etl.api.pipeline.Distinct;
import etl.api.pipeline.Join;
import etl.api.pipeline.Map;
import etl.api.pipeline.PartitionMap;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.Split;
import etl.api.pipeline.TransformVisitor;
import etl.api.pipeline.Union;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import etl.spark.pipeline.transform.parser.AggregatorParser;
import etl.spark.pipeline.transform.parser.DistinctParser;
import etl.spark.pipeline.transform.parser.JoinParser;
import etl.spark.pipeline.transform.parser.MapParser;
import etl.spark.pipeline.transform.parser.PartitionMapParser;
import etl.spark.pipeline.transform.parser.SourceLoaderParser;
import etl.spark.pipeline.transform.parser.SplitParser;
import etl.spark.pipeline.transform.parser.UnionParser;
import org.apache.spark.sql.SparkSession;

public final class Interpreter implements TransformVisitor {

    private final SparkSession sparkSession;

    private final TransformDatasetService transformDatasetService;

    private final StepExecution stepExecution;

    public Interpreter( StepExecution stepExecution,
                        SparkSession sparkSession,
						TransformDatasetService transformDatasetService) {
        this.stepExecution = stepExecution;
        this.sparkSession = sparkSession;
        this.transformDatasetService = transformDatasetService;

    }

    @Override
    public void visit(Map map) {
        new MapParser(transformDatasetService,stepExecution).parse(map);
    }

    @Override
    public void visit(Distinct distinct) {
        new DistinctParser(transformDatasetService,stepExecution).parse(distinct);
    }

    @Override
    public void visit(Aggregator aggregator) {
        new AggregatorParser(transformDatasetService,stepExecution).parse(aggregator);
    }

    @Override
    public void visit(PartitionMap partitionMap) {
        new PartitionMapParser(transformDatasetService,stepExecution).parse(partitionMap);
    }

    @Override
    public void visit(Join join) {
        new JoinParser(transformDatasetService,stepExecution).parse(join);
    }

    @Override
    public void visit(Union union) {
        new UnionParser(transformDatasetService,stepExecution).parse(union);
    }

    @Override
    public void visit(Split split) {
        new SplitParser(transformDatasetService,stepExecution).parse(split);
    }

    @Override
    public void visit(SourceLoader sourceLoader) {
        new SourceLoaderParser(transformDatasetService,sparkSession,stepExecution).parse(sourceLoader);
    }
}
