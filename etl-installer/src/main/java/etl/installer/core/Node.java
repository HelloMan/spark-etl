package etl.installer.core;

import com.google.common.collect.ComparisonChain;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@EqualsAndHashCode(of = "host")
public class Node  implements Comparable<Node>{
    public static final Node LOCALHOST = new Node("localhost");
    @Getter
    private final String host;
    private final Set<Service> masters = new HashSet<>();
    private final Set<Service> slaves = new HashSet<>();

    private final Set<Service> clients = new HashSet<>();

    public Node(String host) {
        this.host = host;
    }

    public void addMaster(Service service) {
        this.masters.add(service);
    }

    public void addSlave(Service service) {
        this.slaves.add(service);
    }

    public void addClient(Service service) {
        this.clients.add(service);
    }

    @Override
    public int compareTo(Node o) {
        return ComparisonChain.start().compare(this.host, o.host).result();
    }
}
