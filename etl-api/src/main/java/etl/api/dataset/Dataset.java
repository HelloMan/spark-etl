package etl.api.dataset;

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

@Getter
@Setter
@Entity
@Table(name = "DATASET")
public  class Dataset {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ID",nullable = false)
    private Long id;

    @Column(name = "TABLE_NAME")
    private String table;

    @Column(name = "NAME")
    private String name;

    @Column(name = "DATASET_TYPE")
    @Enumerated(EnumType.STRING)
    private DatasetType datasetType;

    @Column(name = "JOB_EXECUTION_ID")
    private long jobExecutionId;

    @Column(name = "CREATED_TIME")
    private Date createdTime;


    @Column(name = "METADATA_KEY")
    private String metadataKey;

    @Column(name = "METADATA_CONTENT")
    @Lob
    private String metadata;

	@Column(name = "RECORDS_COUNT")
	private Long recordsCount;

	@Column(name = "PREDICATE")
	private String predicate;


}