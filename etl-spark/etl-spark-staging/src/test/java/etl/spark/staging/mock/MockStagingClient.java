package etl.spark.staging.mock;

import etl.client.StagingClient;
import okhttp3.Request;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.io.IOException;
@Component
@Primary
public class MockStagingClient implements StagingClient {
    @Override
    public Call<Void> updateDataSetState(@Path("datasetId") long datasetId,
										 @Query("state") String state) {
        return new Call<Void>() {
            @Override
            public Response<Void> execute() throws IOException {
                return Response.success(null);
            }

            @Override
            public void enqueue(Callback<Void> callback) {

            }

            @Override
            public boolean isExecuted() {
                return false;
            }

            @Override
            public void cancel() {

            }

            @Override
            public boolean isCanceled() {
                return false;
            }

            @Override
            public Call<Void> clone() {
                return null;
            }

            @Override
            public Request request() {
                return null;
            }
        };
    }
}
