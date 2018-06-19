package etl.spark.staging.mock;

import etl.api.table.Table;
import etl.client.TableClient;
import etl.spark.staging.utils.TableFactory;
import okhttp3.Request;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.http.Path;

import java.io.IOException;
@Component
@Primary
public class MockTableClient implements TableClient {
    @Override
    public Call<Table> getTable(@Path("tableName") String tableName) {
        return new Call<Table>() {
            @Override
            public Response<Table> execute() throws IOException {
                return Response.success(TableFactory.createEmployeeSchema());
            }

            @Override
            public void enqueue(Callback<Table> callback) {

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
            public Call<Table> clone() {
                return null;
            }

            @Override
            public Request request() {
                return null;
            }
        };
    }
}
