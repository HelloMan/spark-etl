package etl.api.pipeline;

import etl.api.AbstractJavaBeanTest;

/**
 * Created by Jack Yang on 9/6/2017.
 */
public class OrderByTest extends AbstractJavaBeanTest<OrderBy> {
    @Override
    protected OrderBy getBeanInstance() {
        return new OrderBy();
    }

    @Override
    protected String[] getEqualsIgnoredFields() {
        return new String[] {"asc"};
    }


}
