package etl.spark.staging.mock;

import etl.api.dataset.Dataset;
import etl.api.parameter.Parameters;
import etl.client.DatasetClient;
import etl.spark.staging.utils.DatasetFactory;
import okhttp3.Request;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.http.Body;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.io.IOException;

@Component
@Primary
public class MockDatasetClient implements DatasetClient {

	@Override
    public Call<Dataset> getLastVersionOfDataset(@Path("datasetName") String datasetName, @Query("metadata") Parameters metadata) {
        return new Call<Dataset>() {
            @Override
            public Response<Dataset> execute() throws IOException {
                return Response.success(DatasetFactory.createDataset(datasetName, metadata));
            }

            @Override
            public void enqueue(Callback<Dataset> callback) {

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
            public Call<Dataset> clone() {
                return null;
            }

            @Override
            public Request request() {
                return null;
            }
        };
    }

    @Override
    public Call<Dataset> createDataset(@Body Dataset dataset) {
        return new Call<Dataset>() {
            @Override
            public Response<Dataset> execute() throws IOException {
                dataset.setId(1l);
                return Response.success(dataset);
            }

            @Override
            public void enqueue(Callback<Dataset> callback) {

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
            public Call<Dataset> clone() {
                return null;
            }

            @Override
            public Request request() {
                return null;
            }
        };
    }
}
