package etl.spark.staging.execution;

public interface StagingListener {

    void onValidationStart(long stagingDatasetId);

    void onValidationFinished(long stagingDatasetId);

    void onValidationFailed(long stagingDatasetId);

    void onRegisterStart(long stagingDatasetId);

    void onRegisterFinished(long stagingDatasetId);

    void onRegisterFailed(long stagingDatasetId);
}
