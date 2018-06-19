package etl.server.service.common;

import etl.server.util.spark.SparkSubmitOption;
import etl.server.util.spark.SparkYarnSubmitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.StoppableTasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.io.IOException;

@Slf4j
public abstract class StoppableSparkJobTasklet implements StoppableTasklet {
    @Autowired
    private YarnClient yarnClient;


    @Autowired
    private YarnApplicationIdHolder yarnApplicationIdHolder;


    @Override
    public void stop() {
        ApplicationId applicationId = yarnApplicationIdHolder.getValue();
        if (applicationId != null) {
            try {
                yarnClient.killApplication(applicationId);
            } catch (YarnException |IOException e) {
                log.error(String.format("An error occurred while stop spark application with application id=%s", applicationId.toString()), e);
            }
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try (SparkYarnSubmitter sparkYarnSubmitter = new SparkYarnSubmitter(createSparkSubmitOption(contribution,chunkContext))) {
            submitAndMonitorApplication(contribution,sparkYarnSubmitter);
        }
        return RepeatStatus.FINISHED;
    }

    protected void submitAndMonitorApplication(StepContribution contribution,SparkYarnSubmitter sparkYarnSubmitter) {
        ApplicationId applicationId = sparkYarnSubmitter.submitApplication();
        yarnApplicationIdHolder.setValue(applicationId);
        try {
            Tuple2<YarnApplicationState, FinalApplicationStatus> finalResult = sparkYarnSubmitter.monitorApplication(applicationId);
            setJobExitStatus(contribution, finalResult);
        } catch (Exception e) {
            log.warn(String.format("An error occurred while monitor spark application with id=%s", applicationId.toString()), e);
        }
    }

    protected void setJobExitStatus(StepContribution contribution, Tuple2<YarnApplicationState, FinalApplicationStatus> finalResult) {
        switch (finalResult._1()) {
            case FAILED:
                contribution.setExitStatus(ExitStatus.FAILED);
                break;
            case FINISHED:
                switch (finalResult._2()) {
                    case FAILED:
                        contribution.setExitStatus(ExitStatus.FAILED);
                        break;
                            /** Application which was terminated by a user or admin. */
                    case KILLED :
                        contribution.setExitStatus(ExitStatus.STOPPED);
                        break;
                    case SUCCEEDED:
                        contribution.setExitStatus(ExitStatus.COMPLETED);
                        break;
                    case UNDEFINED:
                        contribution.setExitStatus(ExitStatus.UNKNOWN);
                };
                break;
            case KILLED:
                contribution.setExitStatus(ExitStatus.STOPPED);
                break;
            default:
                contribution.setExitStatus(ExitStatus.UNKNOWN);
        }
    }

    protected  abstract SparkSubmitOption createSparkSubmitOption(StepContribution contribution, ChunkContext chunkContext) throws IOException;
}
