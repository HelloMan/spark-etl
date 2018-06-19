package etl.spark.pipeline.dq;

import etl.api.job.JobType;
import etl.spark.pipeline.core.AbstractJobOperator;
import etl.spark.pipeline.core.JobListener;
import etl.spark.pipeline.core.StepListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DataQualityJobOperator extends AbstractJobOperator {

    @Autowired
    private JobListener jobListener;

    @Autowired
    private StepListener stepListener;


    @Override
    public JobListener getJobListener() {
        return jobListener;
    }

    @Override
    public StepListener getStepListener() {
        return stepListener;
    }

    @Override
    public JobType getJobType() {
        return JobType.DATA_QUALITY_CHECK;
    }
}
