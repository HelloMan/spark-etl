package etl.spark.staging.exception;

import etl.spark.core.DriverException;

public class StagingDriverException extends DriverException {
    public StagingDriverException(Throwable cause) {
        super(cause);
    }


    public StagingDriverException(String message) {
        super(message);
    }

    public StagingDriverException(String message, Throwable cause) {
        super(message, cause);
    }

    public StagingDriverException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
