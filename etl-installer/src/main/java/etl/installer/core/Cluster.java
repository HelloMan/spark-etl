package etl.installer.core;


import com.izforge.izpack.api.data.InstallData;
import etl.installer.panel.VariableConstants;
import lombok.Getter;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Getter
public class Cluster {
    private final SortedSet<Node> nodes ;
    private final Set<Service> services ;

    public Cluster(String... hosts) {
        this.nodes = new TreeSet<>(Arrays.stream(hosts).map(Node::new).collect(Collectors.toSet()));
        this.services = createServices();
    }
    public  Cluster(InstallData installData) {
        this.nodes = new TreeSet<>(Arrays.stream(installData.getVariable("hosts").split(",")).map(Node::new).collect(Collectors.toSet()));
        this.services = createServices(installData);
    }

    private Set<Service> createServices(InstallData installData) {
        final Set<Service> result = new HashSet<>();
        result.addAll(buildServices(installData, VariableConstants.HDFS_NN_HOST, Component.NAMENODE));

        result.addAll(buildServices(installData, VariableConstants.HDFS_SN_HOST, Component.SECONDARY_NAMENODE));
        result.addAll(buildServices(installData, VariableConstants.YARN_RM_HOST, Component.RESOURCE_MANAGER));
        result.addAll(buildServices(installData, VariableConstants.HISTORY_SERVER_HOST, Component.HISTORY_SERVER));
        result.addAll(buildServices(installData, VariableConstants.HBASE_MASTER_HOST, Component.HMASTER));
        result.addAll(buildServices(installData, VariableConstants.HBASE_BACKUP_MASTER_HOSTS, Component.HMASTER_BACKUP));
        result.addAll(buildServices(installData, VariableConstants.ZOOKEEPER_HOSTS, Component.ZOOKEEPER));

        result.addAll(buildServices(installData, VariableConstants.HDFS_DN_HOSTS, Component.DATANODE));
        result.addAll(buildServices(installData, VariableConstants.YARN_NM_HOSTS, Component.NODE_MANAGER));
        result.addAll(buildServices(installData, VariableConstants.HBASE_REGIONSERVER_HOSTS, Component.REGION_SERVER));
        result.addAll(buildServices(installData, VariableConstants.PHOENIX_QS_HOSTS, Component.QUERY_SERVER));
        result.addAll(buildServices(installData, VariableConstants.IGNIS_HOST, Component.IGNIS));
        return result;
    }

    private  Set<Service> buildServices(InstallData installData,String hostVariable, Component component) {
        return Arrays.stream(installData.getVariable(hostVariable).split(","))
                .map(this::getNode)
                .map(n -> new Service(n, component))
                .collect(Collectors.toSet());
    }


    private Set<Service> createServices() {
        final Set<Service> result = new HashSet<>();
        NodeAllocation nodeAllocation = new RoundRobinAllocation(nodes);

        result.addAll(Component.getMasters().stream().map(master -> new Service(nodeAllocation.allocate(), master)).collect(Collectors.toList()));

        result.addAll(Component.getSlaves().stream().flatMap(slave -> nodes.stream().map(node -> new Service(node, slave))).collect(Collectors.toList()));

        result.addAll(Component.getClients().stream().map(client -> new Service(nodeAllocation.allocate(), client)).collect(Collectors.toList()));

        return result;
    }

    public Node getNode(String host) {
        return nodes.stream().filter(node -> node.getHost().equals(host)).findFirst().orElse(Node.LOCALHOST);
    }

    interface NodeAllocation {
        Node allocate();
    }

    /**
     * from first to last and then back to first
     */
    class RoundRobinAllocation implements NodeAllocation {
        private Deque<Node> queue;
        private final Collection<Node> nodes;
        public RoundRobinAllocation(Collection<Node> nodes) {
            queue = new ArrayDeque<>(nodes);
            this.nodes = nodes;
        }

        @Override
        public Node allocate() {
            if (queue.isEmpty()) {
                queue = new ArrayDeque<>(nodes);
            }
            return queue.pop();
        }
    }
    public String getHost(Component component) {
        Node node = services.stream().filter(s -> s.getComponent().equals(component)).findFirst().map(Service::getNode).orElse(Node.LOCALHOST);
        return node.getHost();
    }

    public Set<String> getHosts(Component component) {
        return services.stream().filter(s -> s.getComponent().equals(component)).map(s -> s.getNode().getHost()).collect(Collectors.toSet());
    }

}
