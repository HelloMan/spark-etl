package etl.client;

import org.junit.Test;

/**
 * Created by chaojun on 2017/11/11.
 */
public class TableClientTest {

    @Test
    public void testClient() {
        TableClient tableClient = RetrofitFactory.getInstance("http://namenode:8080").create(TableClient.class);
        tableClient.getTable("employee");
    }
}
