package etl.api.job.pipeline;

import etl.api.AbstractJavaBeanTest;

/**
 * Created by Jack Yang on 9/6/2017.
 */
public class PipelineStepTest extends AbstractJavaBeanTest<PipelineStep> {
    @Override
    protected PipelineStep getBeanInstance() {
        return new PipelineStep();
    }

    @Override
    protected String[] getEqualsIgnoredFields() {
        return new String[] {"description","transforms"};
    }

}
