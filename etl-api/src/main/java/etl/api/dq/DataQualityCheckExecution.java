package etl.api.dq;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Getter
@Setter
@Entity
@Table(name = "DATA_QUALITY_CHECK_EXECUTION")
public class DataQualityCheckExecution {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "ID", nullable = false)
	private Long id;

	@Column(name = "JOB_EXECUTION_ID")
	private long jobExecutionId;

	@Column(name = "STATUS", length = 20)
	@Enumerated(EnumType.STRING)
	private DataQualityCheckState status;

	@Column(name = "START_TIME")
	private Date startTime;

	@Column(name = "LAST_UPDATE_TIME")
	private Date lastUpdateTime;

	@Column(name = "END_TIME")
	private Date endTime;

	@Column(name = "FAILED_RECORD_NUMBER")
	private long failedRecordNumber;

	@Column(name = "NAME")
	private String name;

	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "TARGET_FILE")
	private String targetFile;

}