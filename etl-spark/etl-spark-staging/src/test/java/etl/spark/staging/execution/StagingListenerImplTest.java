package etl.spark.staging.execution;

import etl.spark.TestStagingApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestStagingApplication.class)
public class StagingListenerImplTest {


    @Autowired
    private StagingListener listener;


    @Test
    public void validationStart() throws Exception {
        listener.onValidationStart(1l);

    }

    @Test
    public void validationFinished() throws Exception {
        listener.onValidationFinished(1l);
    }

    @Test
    public void validationFailed() throws Exception {
        listener.onValidationFailed(1l);
    }

    @Test
    public void registerStart() throws Exception {
        listener.onRegisterStart(1l);
    }

    @Test
    public void registerFinished() throws Exception {
        listener.onRegisterFinished(1l);
    }

    @Test
    public void registerFailed() throws Exception {
        listener.onRegisterFailed(1l);
    }
}