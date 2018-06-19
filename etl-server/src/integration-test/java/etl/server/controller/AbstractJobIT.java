package etl.server.controller;

import com.google.common.collect.ImmutableList;
import etl.api.dataset.DatasetMetadata;
import etl.api.datasource.CsvDataSource;
import etl.api.job.staging.StagingItemRequest;
import etl.api.job.staging.StagingJobRequest;
import etl.api.table.DateField;
import etl.api.table.IntField;
import etl.api.table.StringField;
import etl.api.table.Table;
import etl.common.hdfs.FileSystemTemplate;
import etl.server.annotation.DataSourceQualifier;
import etl.server.service.dataset.DatasetService;
import etl.server.service.staging.impl.job.DatasetConf;
import etl.server.service.table.TableService;
import etl.server.utils.YarnClientTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class AbstractJobIT {
	protected static final String URL_START_STAGING_JOB = "/v1/staging/jobs";
	protected static final String URL_GET_STAGING_JOB = "/v1/staging/jobs/%d";

	protected static final String URL_START_DQ_JOB = "/v1/dq/jobs";
	protected static final String URL_GET_DQ_JOB = "/v1/dq/jobs/%s/rules";

	protected static final String STEP_NAME = "step";
	protected static final String SCHEMA_NAME = "employee";

	@Autowired
	protected TestRestTemplate restTemplate;

	@Autowired
	protected FileSystemTemplate fileSystemTemplate;

	@Autowired
	protected DatasetConf datasetConf;

	@Autowired
	protected YarnClientTemplate yarnClientTemplate;

	@Autowired
	protected TableService tableService;
	@Autowired
	@DataSourceQualifier(DataSourceQualifier.Target.PHOENIX)
	protected JdbcTemplate phoenixJdbcTemplate;

	@Autowired
	protected DatasetService datasetService;

	protected void init() throws Exception {
		yarnClientTemplate.killRunningApplications();
	}

	protected Table createEmployeeSchema() {
		Table table = new Table();
		table.setName(SCHEMA_NAME);

		IntField f1 = new IntField();
		f1.setName("id");
		f1.setKey(true);
		f1.setNullable(false);

		StringField f2 = new StringField();
		f2.setName("name");
		f2.setKey(false);
		f2.setNullable(false);
		f2.setMaxLength(50);
		f2.setMinLength(1);

		IntField f3 = new IntField();
		f3.setName("age");
		f3.setKey(false);
		f3.setNullable(false);

		DateField f4 = new DateField();
		f4.setName("birthDay");
		f4.setKey(false);
		f4.setNullable(true);
		f4.setFormat("yyyy-MM-dd");

		table.setFields(ImmutableList.of(f1, f2, f3, f4));
		return table;
	}

	protected StagingJobRequest createEmployeeStagingJobRequest(Table schema, String datasetFile) {
		return StagingJobRequest.builder()
				.name("staging")
				.item(StagingItemRequest.builder()
						.dataset(DatasetMetadata.of(SCHEMA_NAME))
						.table(SCHEMA_NAME)
						.source(CsvDataSource.builder().header(false).filePath(datasetFile).build())
						.build())
				.build();
	}

	protected void cleanupHdfsFiles(List<String> hdfsFiles) {
		hdfsFiles.forEach(file -> {
			try {
				if (StringUtils.isNoneBlank(file)) {
					fileSystemTemplate.delete(new Path(file), true);
				}
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		});
	}

	protected void dropAllHiveTables() {
		datasetService.getAllDatasets().forEach(ds -> {
			if (ds != null) {
				dropTable(ds);
			}
		});
	}


	protected void dropTable(String tableName) {
		phoenixJdbcTemplate.execute("drop table if exists " + tableName.toLowerCase());
	}

	protected boolean tableExists(String tableName) throws SQLException {
		// not work using: jdbcTemplate.getDataSource().getConnection().getMetaData();
		List<Map<String, Object>> res = phoenixJdbcTemplate.queryForList("show tables");
		return res.stream().anyMatch(row -> row.get("tableName").toString().equalsIgnoreCase(tableName));
	}

}