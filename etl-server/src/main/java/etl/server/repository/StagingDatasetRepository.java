package etl.server.repository;

import etl.server.domain.entity.StagingDataset;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QueryDslPredicateExecutor;

import java.util.List;

public interface StagingDatasetRepository extends JpaRepository<StagingDataset, Long> , QueryDslPredicateExecutor<StagingDataset>{

    List<StagingDataset> findByJobExecutionId(long jobExecutionId);

    StagingDataset findByJobExecutionIdAndDatasetNameAndMetadataKey(long jobExecutionId,String datasetName,String metadataKey);

    List<StagingDataset> findByJobExecutionIdAndDatasetName(long jobExecutionId,String datasetName);

}
