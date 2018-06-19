package etl.server.exception.staging;


public class NoSuchDatasetException extends Exception {
    public NoSuchDatasetException(long stagingDatasetId) {
        super(String.format("Dataset can not be found with stagingDatasetId:%d ", stagingDatasetId));
    }
    public NoSuchDatasetException(long jobExecutionId, String tableName) {
        super(String.format("Dataset can not be found with [jobExecutionId:%d, table:\"%s\"]", jobExecutionId, tableName));
    }

    public NoSuchDatasetException(String tableName) {
        super(String.format("Dataset can not be found with  table:\"%s\"]", tableName));
    }
}
