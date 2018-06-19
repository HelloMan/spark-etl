package etl.api.pipeline;


import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class JoinOnTest extends AbstractJavaBeanTest<JoinOn> {

    @Override
    protected JoinOn getBeanInstance() {
        return new JoinOn();
    }

    @Test
    public void getJoinOn() {
        assertThat(JoinOn.builder().left("left").right("right").build().getLeft()).isEqualTo("left");
        assertThat(JoinOn.builder().left("left").right("right").build().getRight()).isEqualTo("right");
    }
}
