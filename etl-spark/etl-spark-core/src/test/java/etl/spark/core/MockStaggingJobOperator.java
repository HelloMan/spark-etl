package etl.spark.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import etl.api.job.JobType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class MockStaggingJobOperator implements JobOperator {

    @Autowired
    private SparkSession sparkSession;
    @Override
    public void runJob(String... args) throws Exception {
        //do nothing for test
        Objects.requireNonNull(args);
        Preconditions.checkArgument(args.length >= 2);
        Dataset<String> dataset = sparkSession.createDataset(ImmutableList.of(args[0]), Encoders.STRING());
        dataset.registerTempTable(args[1]);

    }

    @Override
    public JobType getJobType() {
        return JobType.STAGING_DATASET;
    }
}
