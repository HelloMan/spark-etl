package etl.server.service.table;

import etl.api.table.Table;
import etl.server.exception.table.NoSuchTableException;
import etl.server.exception.table.TableUpdateException;

import java.util.List;
import java.util.Optional;

public interface TableService {

    Optional<Table> findTable(String tableName) ;

    List<String> getAllTables();

    void dropTable(String tableName) throws NoSuchTableException;

    void saveOrUpdate(Table table) throws TableUpdateException;

}
