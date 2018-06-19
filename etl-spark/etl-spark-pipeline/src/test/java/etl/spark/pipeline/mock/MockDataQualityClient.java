package etl.spark.pipeline.mock;

import etl.api.dq.DataQualityCheckExecution;
import etl.client.DataQualityClient;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Path;


@Component
@Primary
public class MockDataQualityClient implements DataQualityClient {
    @Override
    public Call<Void> updateDQRuleCheckExecution(@Path("jobId") long jobId,
                                                 @Path("ruleName") String ruleName,
                                                 @Body DataQualityCheckExecution ruleExecution) {
        return null;
    }
}
