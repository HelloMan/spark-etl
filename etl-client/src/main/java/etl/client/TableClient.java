package etl.client;

import etl.api.table.Table;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface TableClient {
    @GET("/v1/tables/{tableName}")
    Call<Table> getTable(@Path("tableName") String tableName);
}
