package etl.spark;

import etl.common.annotation.ExcludeFromTest;
import etl.spark.core.DriverContext;
import etl.spark.core.DriverException;
import etl.spark.core.JobOperator;
import lombok.AccessLevel;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@ExcludeFromTest
public class JobLauncher implements CommandLineRunner {

    @Autowired
    @Setter(AccessLevel.PACKAGE)
    private DriverContext driverContext;

    @Autowired
    @Setter(AccessLevel.PACKAGE)
    private List<JobOperator> jobOperators;




    @Override
    public void run(String... args) throws Exception {
        Optional<JobOperator> firstJobOperator = jobOperators.stream()
                .filter(jobOperator1 -> jobOperator1.getJobType().equals(driverContext.getJobType()))
                .findFirst();

        if (firstJobOperator.isPresent()) {
            firstJobOperator.get().runJob(args);
        }else {
            throw new DriverException(String.format("No job operator found for job type %s", driverContext.getJobType()));
        }
    }


}