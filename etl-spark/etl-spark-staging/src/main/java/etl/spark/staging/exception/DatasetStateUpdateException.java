package etl.spark.staging.exception;

public class DatasetStateUpdateException extends StagingDriverException {

	public DatasetStateUpdateException(String msg) {
		super(msg);
	}

    public DatasetStateUpdateException(Throwable cause) {
        super(cause);
    }
}
