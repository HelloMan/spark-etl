CREATE TABLE BATCH_JOB_INSTANCE  (
	JOB_INSTANCE_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT ,
	JOB_NAME VARCHAR(100) NOT NULL,
	JOB_KEY VARCHAR(32) NOT NULL,
	constraint JOB_INST_UN unique (JOB_NAME, JOB_KEY)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION  (
	JOB_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT  ,
	JOB_INSTANCE_ID BIGINT NOT NULL,
	CREATE_TIME DATETIME NOT NULL,
	START_TIME DATETIME DEFAULT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) ,
	EXIT_CODE VARCHAR(2500) ,
	EXIT_MESSAGE VARCHAR(2500) ,
	LAST_UPDATED DATETIME,
	JOB_CONFIGURATION_LOCATION VARCHAR(2500) NULL,
	constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID)
	references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS  (
	JOB_EXECUTION_ID BIGINT NOT NULL ,
	TYPE_CD VARCHAR(6) NOT NULL ,
	KEY_NAME VARCHAR(100) NOT NULL ,
	STRING_VAL VARCHAR(250) ,
	DATE_VAL DATETIME DEFAULT NULL ,
	LONG_VAL BIGINT ,
	DOUBLE_VAL DOUBLE PRECISION ,
	IDENTIFYING CHAR(1) NOT NULL ,
	constraint JOB_EXEC_PARAMS_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION  (
	STEP_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT NOT NULL,
	STEP_NAME VARCHAR(100) NOT NULL,
	JOB_EXECUTION_ID BIGINT NOT NULL,
	START_TIME DATETIME NOT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) ,
	COMMIT_COUNT BIGINT ,
	READ_COUNT BIGINT ,
	FILTER_COUNT BIGINT ,
	WRITE_COUNT BIGINT ,
	READ_SKIP_COUNT BIGINT ,
	WRITE_SKIP_COUNT BIGINT ,
	PROCESS_SKIP_COUNT BIGINT ,
	ROLLBACK_COUNT BIGINT ,
	EXIT_CODE VARCHAR(2500) ,
	EXIT_MESSAGE VARCHAR(2500) ,
	LAST_UPDATED DATETIME,
	constraint JOB_EXEC_STEP_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION_CONTEXT  (
	STEP_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT TEXT ,
	constraint STEP_EXEC_CTX_FK foreign key (STEP_EXECUTION_ID)
	references BATCH_STEP_EXECUTION(STEP_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_JOB_EXECUTION_CONTEXT  (
	JOB_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT TEXT ,
	constraint JOB_EXEC_CTX_FK foreign key (JOB_EXECUTION_ID)
	references BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
) ENGINE=InnoDB;

CREATE TABLE BATCH_STEP_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_STEP_EXECUTION_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_STEP_EXECUTION_SEQ);

CREATE TABLE BATCH_JOB_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_JOB_EXECUTION_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_JOB_EXECUTION_SEQ);

CREATE TABLE BATCH_JOB_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

INSERT INTO BATCH_JOB_SEQ (ID, UNIQUE_KEY) select * from (select 0 as ID, '0' as UNIQUE_KEY) as tmp where not exists(select * from BATCH_JOB_SEQ);

CREATE TABLE `SVC_SERVICE_REQUEST` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`MESSAGE_ID` VARCHAR(255) DEFAULT NULL,
	`SENDER_SYSTEM_ID` VARCHAR(255) DEFAULT NULL,
	`RECEIVER_SERVICE_NAME` VARCHAR(255) DEFAULT NULL,
	`CORRELATION_ID` varchar(255) DEFAULT NULL,
	`NAME` varchar(255) NOT NULL,
	`CREATED_BY` varchar(255),
	`REQUEST_MESSAGE` longtext,
	`JOB_EXECUTION_ID` bigint(20),
	`IS_DELETED` BIT,
	`REQUEST_TYPE` VARCHAR(50),
	PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

CREATE TABLE `DATASET_SCHEMA` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`NAME` varchar(255),
	`CREATED_BY` varchar(50),
	`CREATED_TIME` datetime,
	PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

CREATE TABLE `DATASET_SCHEMA_FIELD` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`NAME` varchar(255),
	`FIELD_TYPE` VARCHAR(50),
	`IS_KEY` BIT,
	`NULLABLE` BIT,
	`DATASET_SCHEMA_ID` bigint(20) NOT NULL,
	`FORMAT` VARCHAR(50),
	`SCALE` INT,
	`PRECISION` INT,
	`MAX_LENGTH` INTEGER,
	`MIN_LENGTH` INTEGER,
	`REGULAR_EXPRESSION` VARCHAR(100),
	 PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

CREATE TABLE `STAGING_DATA_SET` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`END_TIME` datetime DEFAULT NULL,
	`LAST_UPDATE_TIME` datetime DEFAULT NULL,
	`MESSAGE` longtext,
	`TABLE_NAME` varchar(100) DEFAULT NULL,
	`STAGING_FILE` VARCHAR(2000),
	`DATASET` VARCHAR(100),
	`START_TIME` datetime DEFAULT NULL,
	`STATUS` varchar(20) DEFAULT NULL,
	`VALIDATION_ERROR_FILE` VARCHAR(2000),
	`JOB_EXECUTION_ID` bigint(20) DEFAULT NULL,
	`METADATA_KEY` VARCHAR(64) NOT NULL,
	`METADATA_CONTENT` longtext,
	 PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

CREATE TABLE `DATASET` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`JOB_EXECUTION_ID` bigint(20),
	`NAME` varchar(255),
	`TABLE_NAME` varchar(2000),
	`DATASET_TYPE` varchar(20),
	`CREATED_TIME` datetime,
	`METADATA_KEY` VARCHAR(64) NOT NULL,
	`METADATA_CONTENT` longtext,
	`RECORDS_COUNT` BIGINT(20),
	`PREDICATE` varchar(200),
	 PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

CREATE TABLE `DATA_QUALITY_CHECK_EXECUTION` (
	`ID` bigint(20) NOT NULL AUTO_INCREMENT,
	`JOB_EXECUTION_ID` bigint(20),
	`NAME` varchar(255),
  `DESCRIPTION` varchar(2000),
	`STATUS` varchar(20),
	`START_TIME` datetime,
	`LAST_UPDATE_TIME` datetime,
	`END_TIME` datetime,
  `FAILED_RECORD_NUMBER` bigint(20),
	`TARGET_FILE` varchar(2000),
	 PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

