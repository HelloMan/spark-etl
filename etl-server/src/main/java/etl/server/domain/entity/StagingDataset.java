package etl.server.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import etl.api.dataset.DatasetState;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.util.Date;

@Setter
@Getter
@Entity
@Table(name = "STAGING_DATA_SET")
public class StagingDataset {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ID")
    private Long id;

    @Column(name = "START_TIME")
    private Date startTime;

    @Column(name = "END_TIME")
    private Date endTime;

    @Column(name = "MESSAGE")
    @Lob
    private String message;

    @Column(name = "STAGING_FILE")
    private String stagingFile;

    @Column(name = "VALIDATION_ERROR_FILE")
    private String validationErrorFile;

    @Column(name = "TABLE_NAME")
    private String table;

    @Column(name = "STATUS", length = 20)
    @Enumerated(EnumType.STRING)
    private DatasetState status;

    @Column(name = "LAST_UPDATE_TIME")
    private Date lastUpdateTime;

    @Column(name = "JOB_EXECUTION_ID")
    private long jobExecutionId;

    @Column(name = "DATASET")
    private String datasetName;

    @JsonIgnore
    @Column(name = "METADATA_KEY")
    private String metadataKey;

    @Column(name = "METADATA_CONTENT")
    @Lob
    private String metadata;

}
