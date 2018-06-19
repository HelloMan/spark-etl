package etl.server.exception.job;

public class JobStartException extends Exception {
    public JobStartException(String message) {
        super(message);
    }

    public JobStartException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobStartException(Throwable cause) {
        super(cause);
    }
}
