package etl.server.exception.job;


public class NoSuchJobExecutionException extends Exception {

    public NoSuchJobExecutionException(long jobExecutionId) {
        super(String.format("No such job instance with id: %d", jobExecutionId));
    }
}
