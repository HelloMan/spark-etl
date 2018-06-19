package etl.server.util.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import etl.common.json.JsonCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;

/**
 * submit spark job to yarn cluster
 */
@Slf4j
public class SparkYarnSubmitter implements AutoCloseable {

	private static final Logger LOGGER = LoggerFactory.getLogger(SparkYarnSubmitter.class);

    public static final String HADOOP_USER = "hadoop.user";

	private final Client client;
    private final SparkSubmitOption sparkSubmitOption;

    public SparkYarnSubmitter(SparkSubmitOption sparkSubmitOption) {
        client = this.createClient(sparkSubmitOption);
        this.sparkSubmitOption = sparkSubmitOption;
    }

    public SparkYarnSubmitter(Client client,SparkSubmitOption sparkSubmitOption) {
        this.client = client;
        this.sparkSubmitOption = sparkSubmitOption;
    }


    public ApplicationId submitApplication() {
        try{
           return  UserGroupInformation.createRemoteUser(sparkSubmitOption.getHdfsUser())
                    .doAs((PrivilegedAction<ApplicationId>) client::submitApplication);
        }finally {
            FileUtils.deleteQuietly(new File(sparkSubmitOption.getJobStagingBaseDir()));
        }
    }

    public  Tuple2<YarnApplicationState,FinalApplicationStatus> monitorApplication(ApplicationId applicationId) {
        return client.monitorApplication(applicationId, false, true);
    }


    @VisibleForTesting
    Client createClient(SparkSubmitOption sparkSubmitOption) {

        List<String> args = Lists.newArrayList("--jar", sparkSubmitOption.getDriverJar(), "--class", sparkSubmitOption.getClazz());
        sparkSubmitOption.getArgs().forEach(arg -> {
            args.add("--arg");
            args.add(new JsonCodec().encode(arg));
        });

        // identify that you will be using Spark as YARN mode
        System.setProperty("SPARK_YARN_MODE", "true");
        sparkSubmitOption.getSparkConf().set("spark.app.name", sparkSubmitOption.getJobName());
        //identify that cluster mode will be use
        sparkSubmitOption.getSparkConf().set("spark.submit.deployMode", "cluster");
        sparkSubmitOption.getSparkConf().set("spark.yarn.submit.waitAppCompletion", "true");
        sparkSubmitOption.getSparkConf().set(HADOOP_USER, sparkSubmitOption.getHdfsUser());
        List<String> distFiles = sparkSubmitOption.getFiles();
        if (CollectionUtils.isNotEmpty(distFiles)) {
            String files = String.join(",", distFiles);
            sparkSubmitOption.getSparkConf().set("spark.yarn.dist.files", files);
        }


        LOGGER.info("Spark args: {}", Arrays.toString(args.toArray()));
        LOGGER.info("Spark conf settings: {}", Arrays.toString(sparkSubmitOption.getSparkConf().getAll()));

        ClientArguments cArgs = new ClientArguments(args.toArray(new String[args.size()]));
        return UserGroupInformation.createRemoteUser(sparkSubmitOption.getHdfsUser())
                .doAs((PrivilegedAction<Client>) () -> new Client(cArgs, sparkSubmitOption.getHadoopConf(), sparkSubmitOption.getSparkConf()));
    }




    @Override
    public void close() throws Exception {
        client.stop();

    }
}
