package etl.server.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class YarnClientTemplate {

    @Autowired
    YarnClient yarnClient;

    public void killRunningApplications() throws IOException, YarnException {
        for (ApplicationReport applicationReport : yarnClient.getApplications()) {
         if (YarnApplicationState.KILLED != applicationReport.getYarnApplicationState()
                 && YarnApplicationState.FAILED != applicationReport.getYarnApplicationState()
                 && YarnApplicationState.FINISHED != applicationReport.getYarnApplicationState()
                 )
             if (!applicationReport.getName().equals("Thrift JDBC/ODBC Server")) {
                 log.warn("Yarn application {} will be killed", applicationReport.getApplicationId().toString());
                 yarnClient.killApplication(applicationReport.getApplicationId());
             }

        }
    }

}
