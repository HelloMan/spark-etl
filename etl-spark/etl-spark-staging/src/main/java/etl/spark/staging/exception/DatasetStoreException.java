package etl.spark.staging.exception;

public class DatasetStoreException extends StagingDriverException {


    public DatasetStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
