package etl.server.controller;

import etl.api.dataset.Dataset;
import etl.server.service.dataset.DatasetService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatasetControllerTest {

    @InjectMocks
    private DatasetController datasetController;

    @Mock
    private DatasetService datasetService;

    @Test
    public void testCreateDataset() throws Exception {
        Dataset dataset = new Dataset();
        datasetController.createDataset(dataset);
        Mockito.verify(datasetService).createDataset(dataset);
    }
}