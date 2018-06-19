package etl.client;

import etl.common.annotation.DeveloperApi;
import retrofit2.Call;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface StagingClient {

    @PUT("/v1/staging/datasets/{datasetId}")
    @DeveloperApi
    Call<Void> updateDataSetState(@Path("datasetId") long datasetId, @Query("state") String state);
}
