package etl.server.exception.dq;

import etl.api.dq.DataQualityCheckState;

public class DQRuleInstanceStatusChangeException extends Exception {

	public DQRuleInstanceStatusChangeException(long jobExecutionId,
			DataQualityCheckState fromStatus,
			DataQualityCheckState toStatus,
			String ruleName) {
		super(String.format("Rule instance status cannot change from %s to %s, rule: %s, job execution id: %s",
				fromStatus, toStatus, ruleName, jobExecutionId));
	}

}