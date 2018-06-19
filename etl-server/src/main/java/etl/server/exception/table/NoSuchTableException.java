package etl.server.exception.table;

public class NoSuchTableException extends Exception {
    public NoSuchTableException(String message) {
        super(message);
    }

    public NoSuchTableException(String message, Throwable cause) {
        super(message, cause);
    }


}
