package etl.server.controller;

import etl.api.table.Table;
import etl.server.service.table.TableService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableControllerTest {

    @InjectMocks
    private TableController tableController;

    @Mock
    private TableService tableService;

    @Test
    public void testSaveOrUpdateTable() throws Exception {
        Table table = new Table();
        tableController.saveOrUpdateTable(table);
        Mockito.verify(tableService).saveOrUpdate(table);
    }
}