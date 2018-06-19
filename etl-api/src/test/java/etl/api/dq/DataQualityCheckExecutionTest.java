package etl.api.dq;

import etl.api.AbstractJavaBeanTest;

/**
 * Created by Jack Yang on 9/6/2017.
 */
public class DataQualityCheckExecutionTest extends AbstractJavaBeanTest<DataQualityCheckExecution> {
    @Override
    protected DataQualityCheckExecution getBeanInstance() {
        return new DataQualityCheckExecution();
    }


}
