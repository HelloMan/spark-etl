package etl.server.exception.table;

public class TableDropException extends Exception {
    public TableDropException(String message) {
        super(message);
    }

    public TableDropException(String message, Throwable cause) {
        super(message, cause);
    }


}
