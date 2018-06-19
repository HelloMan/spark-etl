package etl.spark.config;

import etl.client.*;
import etl.client.*;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.Retrofit;

@Configuration
@Slf4j
public class IgnisClientConfig {

    @Value("${etl.http.web.address}")
    @Setter(AccessLevel.PACKAGE)
    private String httpWebAddress;


    @Bean
    public Retrofit retrofit() {
        return RetrofitFactory.getInstance(httpWebAddress);
    }

    @Bean
    public TableClient tableClient(Retrofit retrofit) {
        return retrofit.create(TableClient.class);
    }

    @Bean
    public DatasetClient datasetClient(Retrofit retrofit) {
        return retrofit.create(DatasetClient.class);
    }

    @Bean
    public DataQualityClient dataQualityClient(Retrofit retrofit) {
        return retrofit.create(DataQualityClient.class);
    }

    @Bean
    public StagingClient stagingClient(Retrofit retrofit) {
        return retrofit.create(StagingClient.class);
    }

}
