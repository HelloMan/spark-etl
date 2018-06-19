package etl.server.exception.table;

public class TableUpdateException extends Exception {
    public TableUpdateException(String message) {
        super(message);
    }

    public TableUpdateException(String message, Throwable cause) {
        super(message, cause);
    }


}
