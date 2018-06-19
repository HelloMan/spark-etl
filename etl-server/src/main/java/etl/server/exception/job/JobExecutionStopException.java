package etl.server.exception.job;

public class JobExecutionStopException extends Exception {
    public JobExecutionStopException(String message) {
        super(message);
    }

    public JobExecutionStopException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobExecutionStopException(Throwable cause) {
        super(cause);
    }
}
