package etl.client;

import etl.api.dataset.Dataset;
import etl.api.parameter.Parameters;
import etl.common.annotation.DeveloperApi;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface DatasetClient {

    @GET("/v1/datasets/{datasetName}/lastVersion")
    Call<Dataset> getLastVersionOfDataset(@Path("datasetName") String datasetName, @Query("metadata") Parameters metadata);

    @Headers("Content-Type:application/json")
    @POST("/v1/datasets")
    @DeveloperApi
    Call<Dataset> createDataset(@Body Dataset dataset);

}
