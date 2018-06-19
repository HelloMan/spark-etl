package etl.server.controller;

import etl.api.table.Table;
import etl.server.exception.table.NoSuchTableException;
import etl.server.exception.table.TableUpdateException;
import etl.server.service.table.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TableController {

    @Autowired
    private TableService tableService;

    @RequestMapping(value = "/v1/tables",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public void saveOrUpdateTable(@RequestBody Table table) throws TableUpdateException {
        tableService.saveOrUpdate(table);
    }
    @RequestMapping(value = "/v1/tables/{tableName}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Table getTable(@PathVariable String tableName) throws NoSuchTableException {
       return tableService.findTable(tableName)
                .orElseThrow(() -> new NoSuchTableException(String.format("table %s can not be found", tableName)));

    }

}
