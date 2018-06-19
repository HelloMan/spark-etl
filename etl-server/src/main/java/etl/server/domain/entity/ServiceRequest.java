package etl.server.domain.entity;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "SVC_SERVICE_REQUEST")
public class ServiceRequest {

    public final static String SERVICE_REQUEST_ID = "serviceRequestId";
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ID",nullable = false)
    private Long id;

    @Column(name = "MESSAGE_ID")
    private String messageId;

    @Column(name = "SENDER_SYSTEM_ID")
    private String senderSystemId;

    @Column(name = "RECEIVER_SERVICE_NAME")
    private String receiverServiceName;

    @Column(name = "CORRELATION_ID")
    private String correlationId;

    @Column(name = "NAME")
    private String name;

    @Column(name = "CREATED_BY")
    private String createdBy;

    @Column(name = "REQUEST_MESSAGE")
    @Lob
    private String requestMessage;

    @Column(name = "JOB_EXECUTION_ID")
    private Long jobExecutionId;

    @Column(name="REQUEST_TYPE")
    @Enumerated(EnumType.STRING)
    private ServiceRequestType serviceRequestType;



}
