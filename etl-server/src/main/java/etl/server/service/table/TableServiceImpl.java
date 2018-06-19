package etl.server.service.table;

import etl.api.table.Table;
import etl.server.exception.table.NoSuchTableException;
import etl.server.exception.table.TableUpdateException;
import etl.server.repository.TableRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
@Transactional
public class TableServiceImpl implements TableService {


    @Autowired
    private TableRepository tableRepository;

    @Override
    public Optional<Table> findTable(String tableName) {
        return Optional.ofNullable(tableRepository.findByName(tableName));
    }

    @Override
    public List<String> getAllTables() {
        return tableRepository.getTableNames();
    }

    @Override
    public void dropTable(String tableName) throws NoSuchTableException {
        Table table = this.findTable(tableName).orElseThrow(() -> new NoSuchTableException(tableName));
        tableRepository.delete(table);
    }

    @Override
    public void saveOrUpdate(Table table) throws TableUpdateException {
        Optional.ofNullable(tableRepository.findByName(table.getName()))
                .ifPresent(tableRepository::delete);
        table.setCreatedTime(new Date());
        table.getFields().forEach(field -> field.setTable(table));
        tableRepository.save(table);
    }


}
