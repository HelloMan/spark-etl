package etl.server.repository;

import etl.api.dataset.Dataset;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

public interface DatasetRepository extends JpaRepository<Dataset, Long> {

    List<Dataset> findByJobExecutionId(long jobExecutionId);

    List<Dataset> findByNameAndMetadataKey(String name, String metadataKey);

    @Query("SELECT a from Dataset a where a.name=:name and a.jobExecutionId=:jobExecutionId and a.metadataKey=:metadataKey")
    Dataset findDataset( @Param("jobExecutionId") long jobExecutionId,@Param("name") String name, @Param("metadataKey") String metadataKey);

    @Query("select a from Dataset a where a.id = (select max(b.id) from Dataset b where b.name =:name and b.metadataKey=:metadataKey)")
    Dataset findLastDatasetOf(@Param("name") String table, @Param("metadataKey") String metadataKey);


    @Query("select a.name from Dataset a ")
    Set<String> findAllDatasets();

    List<Dataset> findByName(String name);

    List<Dataset> findByTable(String table);

}
