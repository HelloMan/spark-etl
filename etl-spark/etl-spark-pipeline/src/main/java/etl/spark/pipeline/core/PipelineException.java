package etl.spark.pipeline.core;

import etl.spark.core.DriverException;

public class PipelineException extends DriverException {

    public PipelineException(String message) {
        super(message);
    }

    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineException(Throwable cause) {
        super(cause);
    }
}
