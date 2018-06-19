package etl.spark.staging.execution;

import etl.api.dataset.DatasetState;
import etl.client.StagingClient;
import etl.spark.staging.exception.StagingDriverException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class StagingListenerImpl implements StagingListener {

	@Autowired
	private StagingClient stagingClient;

	private void updateDatasetState(long stagingDatasetId,DatasetState state){
		try {
			stagingClient.updateDataSetState(stagingDatasetId, state.name()).execute();
		} catch (IOException e) {
			throw new StagingDriverException(e);
		}
	}

	public void onValidationStart(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId,  DatasetState.VALIDATING);
	}

	public void onValidationFinished(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId, DatasetState.VALIDATED);
	}

	public void onValidationFailed(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId, DatasetState.VALIDATION_FAILED);

	}

	public void onRegisterStart(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId, DatasetState.REGISTERING);
	}

	public void onRegisterFinished(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId, DatasetState.REGISTERED);
	}

	public void onRegisterFailed(long stagingDatasetId) {
		updateDatasetState(stagingDatasetId, DatasetState.REGISTRATION_FAILED);
	}



}