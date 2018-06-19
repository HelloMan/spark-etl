package etl.spark.staging.exception;


public class DatasetValidateException extends StagingDriverException {
    public DatasetValidateException(Throwable cause) {
        super(cause);
    }

    public DatasetValidateException(String message) {
        super(message);
    }

    public DatasetValidateException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatasetValidateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
