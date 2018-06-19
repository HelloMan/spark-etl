package etl.server.service.common;

import etl.server.domain.entity.ServiceRequestType;
import etl.server.domain.entity.ServiceRequest;
import etl.server.repository.ServiceRequestRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
public class JobOperatorImplTest {
    @Mock
    private org.springframework.batch.core.launch.JobOperator springJobOperator;

    @Mock
    private ServiceRequestRepository serviceRequestRepository;
    @InjectMocks
    JobOperatorImpl jobOperatorImpl;
    @Test
    public void testStartJob() throws Exception {
        ServiceRequest serviceRequest = new ServiceRequest();
        serviceRequest.setId(1l);

        Mockito.when(serviceRequestRepository.save(Mockito.isA(ServiceRequest.class))).thenReturn(serviceRequest);
        jobOperatorImpl.startJob(ServiceRequestType.PIPELINE.name(), () -> serviceRequest, new Properties());

        Mockito.verify(serviceRequestRepository, new Times(1)).save(Mockito.isA(ServiceRequest.class));
    }
}