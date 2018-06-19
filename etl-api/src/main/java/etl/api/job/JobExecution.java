package etl.api.job;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Optional;

@SuppressWarnings("squid:UndocumentedApi")
@NoArgsConstructor
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobExecution {

    private long jobExecutionId;

    private String jobStatus;

    private String exitStatus;

    private Date startTime;

    private Date finishTime;

    @Builder
    public JobExecution(long jobExecutionId,
                        String jobStatus,
                        String exitStatus,
                        Date startTime,
                        Date finishTime) {
        this.jobExecutionId = jobExecutionId;
        this.jobStatus = jobStatus;
        this.exitStatus = exitStatus;
        this.startTime = Optional.ofNullable(startTime).orElse(null);
        this.finishTime = Optional.ofNullable(finishTime).orElse(null);
    }

}
