package etl.client;

import etl.api.dq.DataQualityCheckExecution;
import etl.common.annotation.DeveloperApi;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.PUT;
import retrofit2.http.Path;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

public interface DataQualityClient {

    @Headers(HttpHeaders.CONTENT_TYPE + ":" + MediaType.APPLICATION_JSON)
    @PUT("/v1/dq/jobs/{jobId}/rules/{ruleName}")
    @DeveloperApi
    Call<Void> updateDQRuleCheckExecution(@Path("jobId") long jobId,@Path("ruleName") String ruleName,
                                          @Body DataQualityCheckExecution ruleExecution);

}
