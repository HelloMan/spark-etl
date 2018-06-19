package etl.server.service.table;

import com.google.common.collect.ImmutableList;
import etl.api.table.Table;
import etl.server.repository.TableRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TableServiceImplTest {
    @Mock
    private TableRepository tableRepository;
    @InjectMocks
    private TableServiceImpl tableService;

    @Test
    public void testFindTable() throws Exception {

        Mockito.when(tableRepository.findByName("employee")).thenReturn((new Table()));
        Optional<Table> result = tableService.findTable("employee");
        assertThat(result.isPresent()).isTrue();
    }

    @Test
    public void testGetAllTables() throws Exception {
        Mockito.when(tableRepository.getTableNames()).thenReturn(ImmutableList.of("employee"));
        assertThat(tableService.getAllTables().size()).isEqualTo(1);
    }

    @Test
    public void testDropTable() throws Exception {
        Table table = new Table();
        Mockito.when(tableRepository.findByName("employee")).thenReturn(table);
        tableService.dropTable("employee");
        verify(tableRepository).delete(table);
    }

    @Test
    public void testSaveOrUpdateTable() throws Exception {
        Table table = new Table();
        table.setName("employee");
        Mockito.when(tableRepository.findByName("employee")).thenReturn(table);

        tableService.saveOrUpdate(table);
        verify(tableRepository).delete(table);
        verify(tableRepository).save(table);
    }


}