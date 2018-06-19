package etl.server.service.staging;

import etl.api.job.staging.StagingJobRequest;
import etl.server.domain.entity.StagingDataset;
import etl.server.exception.job.JobStartException;
import etl.server.exception.staging.DatasetStateChangeException;
import javaslang.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface StagingDatasetService {

    long start(StagingJobRequest jobRequest) throws JobStartException;

    Optional<StagingDataset> findStagingDataset(long stagingDatasetId);

    List<StagingDataset> findStagingDatasets(long jobExecutionId,String dataset);

    Optional<Tuple2<String, InputStream>> getStagingDatasetError(long stagingDatasetId) throws IOException;

    void updateStagingDatasetState(long stagingDatasetId, String state) throws DatasetStateChangeException;


}