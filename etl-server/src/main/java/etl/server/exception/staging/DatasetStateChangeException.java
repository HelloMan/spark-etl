package etl.server.exception.staging;

import etl.api.dataset.DatasetState;

public class DatasetStateChangeException extends Exception {

    public DatasetStateChangeException(String message) {
        super(message);
    }

    public DatasetStateChangeException(long jobExecutionId, String dataset, DatasetState fromState, DatasetState toState) {
        super(String.format("Can't transit dataset state from %s to %s with jobExecutionId:%d,dataset:%s", fromState, toState, jobExecutionId,dataset));
    }

}
