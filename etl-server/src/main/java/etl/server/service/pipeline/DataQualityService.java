package etl.server.service.pipeline;

import etl.api.dq.DataQualityCheckExecution;
import etl.server.exception.dq.DQRuleInstanceStatusChangeException;
import etl.server.exception.dq.NoSuchRuleException;
import javaslang.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;


public interface DataQualityService extends PipelineJobService {

    void updateDataQualityRuleExecution(long jobExecutionId, String ruleName, DataQualityCheckExecution execution)
			throws NoSuchRuleException, DQRuleInstanceStatusChangeException;

    List<DataQualityCheckExecution> getDataQualityRuleExecutions(long jobExecutionId);

    Optional<Tuple2<String, InputStream>> getResultOfDataQualityRuleExecution(long jobExecutionId, String ruleName) throws IOException;
}
