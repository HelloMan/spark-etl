package etl.installer.core;

import com.izforge.izpack.api.data.InstallData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)

public class ClusterTest {

    @Mock
    private InstallData installData;

    @Test
    public void testCreateFromHosts() {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getNodes().size()).isEqualTo(3);
    }

    @Test
    public void testCreateFromInstallData() {
        Mockito.when(installData.getVariable("hosts")).thenReturn("node1,node2,node3");
        Mockito.when(installData.getVariable("hdfs.nn.host")).thenReturn("node1");

        Mockito.when(installData.getVariable("hdfs.sn.host")).thenReturn("node2");
        Mockito.when(installData.getVariable("yarn.rm.host")).thenReturn("node3");
        Mockito.when(installData.getVariable("history.server.host")).thenReturn("node1");
        Mockito.when(installData.getVariable("hbase.master.host")).thenReturn("node2");
        Mockito.when(installData.getVariable("hbase.backup.master.hosts")).thenReturn("node3");
        Mockito.when(installData.getVariable("zookeeper.hosts")).thenReturn("node1,node2,node3");

        Mockito.when(installData.getVariable("hdfs.dn.hosts")).thenReturn("node1,node2,node3");
        Mockito.when(installData.getVariable("yarn.nm.hosts")).thenReturn("node1,node2,node3");
        Mockito.when(installData.getVariable("hbase.regionserver.hosts")).thenReturn("node1,node2,node3");
        Mockito.when(installData.getVariable("phoenix.qs.hosts")).thenReturn("node1,node2,node3");
        Mockito.when(installData.getVariable("etl.host")).thenReturn("node1");
        Cluster cluster = new Cluster(installData);
        assertThat(cluster.getNodes().size()).isEqualTo(3);
        assertThat(cluster.getHost(Component.NAMENODE)).isEqualTo("node1");
    }


    @Test
    public void testGetNode() throws Exception {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getNode("node1").getHost()).isEqualTo("node1");
    }

    @Test
    public void testGetHost() throws Exception {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getHost(Component.NAMENODE)).isEqualTo("node1");
//        assertThat(cluster.getHost(Component.SECONDARY_NAMENODE)).isEqualTo("node1");
    }

    @Test
    public void testGetHosts() throws Exception {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getHosts(Component.DATANODE).size()).isEqualTo(3);
    }

    @Test
    public void testGetNodes() throws Exception {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getNodes().size()).isEqualTo(3);
    }

    @Test
    public void testGetServices() throws Exception {
        Cluster cluster = new Cluster("node1", "node2", "node3");
        assertThat(cluster.getServices().size()).isEqualTo(Component.getMasters().size() + cluster.getNodes().size() * Component.getSlaves().size() + Component.getClients().size());

    }
}