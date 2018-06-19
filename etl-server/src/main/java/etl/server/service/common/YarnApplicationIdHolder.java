package etl.server.service.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;

@StepScope
@Component
@Getter
@Setter
public class YarnApplicationIdHolder {

    private ApplicationId value;
}
