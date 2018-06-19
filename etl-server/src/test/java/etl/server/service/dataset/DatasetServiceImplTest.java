package etl.server.service.dataset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import etl.api.dataset.Dataset;
import etl.api.dataset.DatasetMetadata;
import etl.server.repository.DatasetRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatasetServiceImplTest {
    @Mock
    private DatasetRepository datasetRepository;

    @InjectMocks
    private DatasetServiceImpl datasetService;

    @Test
    public void testCreateDataset() throws Exception {
        Dataset dataset = new Dataset();
        dataset.setName("employee");

        datasetService.createDataset(dataset);
        verify(datasetRepository).save(dataset);
    }

    @Test
    public void testFindLastDataset() throws Exception {
        Dataset dataset = new Dataset();
        dataset.setId(1l);
        DatasetMetadata datasetMetadata = DatasetMetadata.of("table");
        when(datasetRepository.findLastDatasetOf("table",datasetMetadata.toMetadataKey())).thenReturn(dataset);
        Optional<Dataset> employee = datasetService.findLastDataset(datasetMetadata);
        assertThat(employee.isPresent()).isTrue();
        assertThat(employee.get().getId()).isEqualTo(1l);
    }

    @Test
    public void testFindDatasets() throws Exception {
        DatasetMetadata datasetMetadata = DatasetMetadata.of("table");
        when(datasetRepository.findByNameAndMetadataKey("table", datasetMetadata.toMetadataKey())).thenReturn(ImmutableList.of(new Dataset()));
        List<Dataset> result = datasetService.findDatasets(datasetMetadata);
        assertThat(result.size()).isEqualTo(1);
    }
    @Test
    public void testFindDatasetByJobExecutionIdAndTableAndMetadata() throws Exception {
        DatasetMetadata datasetMetadata = DatasetMetadata.of("table");
        when(datasetRepository.findDataset(1l, "table", datasetMetadata.toMetadataKey())).thenReturn(new Dataset());
        assertThat(datasetService.findDataset(1l, datasetMetadata).isPresent()).isTrue();
    }
    @Test
    public void testGetAllDatasets() throws Exception {
        when(datasetRepository.findAllDatasets()).thenReturn(ImmutableSet.of("employee", "dept"));
        Set<String> allDatasets = datasetService.getAllDatasets();
        assertThat(allDatasets.size()).isEqualTo(2);
    }
}