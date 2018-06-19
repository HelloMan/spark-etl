package etl.api.job.staging;

import etl.api.dataset.DatasetMetadata;
import etl.api.datasource.DataSource;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * a domain that present a data set that need to be staging in HDFS
 * @author jason zhang
 */
@Getter
@NoArgsConstructor
@EqualsAndHashCode(of = {"dataset","table"})
public class StagingItemRequest {

    private long id;

    /**
     * a source data set  in staging input directory (e.g. csv file)
     */
    private DataSource source;

    private DatasetMetadata dataset;

    private String table;


    @Builder
    public StagingItemRequest(long id,
                              DataSource source,
                              DatasetMetadata dataset,
                              String table) {
        this.id = id;
        this.source = source;
        this.dataset = dataset;
        this.table = table;

    }




}
