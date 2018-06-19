package etl.server.exception.dataset;

public class DatasetNotFoundException extends RuntimeException {

    public DatasetNotFoundException() {
    }

    public DatasetNotFoundException(String message) {
        super(message);
    }

    public DatasetNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatasetNotFoundException(Throwable cause) {
        super(cause);
    }

    public DatasetNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
