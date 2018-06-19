package etl.installer.core;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.UUID;

@Getter
@EqualsAndHashCode(of = "id")
public class Service {
    private final Node node;
    private final Component component;
    private final String id;

    public Service(Node node, Component component) {
        this.id = UUID.randomUUID().toString();
        this.node = node;
        this.component = component;
        if (component.isMaster()) {
            this.node.addMaster(this);
        }
        if (component.isSlave()) {
            this.node.addSlave(this);
        }
        if (component.isClient()) {
            this.node.addClient(this);
        }
    }


}
