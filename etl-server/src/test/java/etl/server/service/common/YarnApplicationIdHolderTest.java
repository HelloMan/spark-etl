package etl.server.service.common;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by chaojun on 2017/7/23.
 */
public class YarnApplicationIdHolderTest {

    @Test
    public void createYarnApplicationIdHolder(){
        YarnApplicationIdHolder yarnApplicationIdHolder = new YarnApplicationIdHolder();
        yarnApplicationIdHolder.setValue(ApplicationId.newInstance(1l, 1));

        assertThat(yarnApplicationIdHolder).isNotNull();
        assertThat(yarnApplicationIdHolder.getValue().getId()).isEqualTo(1);

    }

}