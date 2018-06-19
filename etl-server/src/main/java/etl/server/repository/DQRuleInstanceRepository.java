package etl.server.repository;

import etl.api.dq.DataQualityCheckExecution;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DQRuleInstanceRepository extends JpaRepository<DataQualityCheckExecution, Long> {

	List<DataQualityCheckExecution> findByJobExecutionId(long jobExecutionId);

	DataQualityCheckExecution findByJobExecutionIdAndName(long jobExecutionId, String name);
}
