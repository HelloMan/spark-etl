package etl.installer.panel.action;

import com.izforge.izpack.api.data.AutomatedInstallData;
import com.izforge.izpack.core.data.DefaultVariables;
import com.izforge.izpack.util.Platform;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class DefaultHostsAssignActionTest {

//    @Test
    public void testExecuteActionWithDistributed() throws Exception {
        AssignHostsAction assignHostsAction = new AssignHostsAction();
        AutomatedInstallData installData = new AutomatedInstallData(new DefaultVariables(), new Platform(Platform.Name.LINUX));
        installData.setVariable("hosts","node1,node2,node3");
        installData.setVariable("distributed","true");
        installData.setVariable("host.count","3");
        installData.setVariable("host.1","node1");
        installData.setVariable("host.2","node2");
        installData.setVariable("host.3","node3");
        assignHostsAction.executeAction(installData, null);
        assertThat(installData.getVariable("hdfs.nn.host")).isEqualTo("node1");
    }

    @Test
    public void testExecuteActionWithStandalone() throws Exception {
        AssignHostsAction assignHostsAction = new AssignHostsAction();
        AutomatedInstallData installData = new AutomatedInstallData(new DefaultVariables(), new Platform(Platform.Name.LINUX));
        installData.setVariable("hosts","node1,node2,node3");
        installData.setVariable("distributed","false");
        assignHostsAction.executeAction(installData, null);
        assertThat(installData.getVariable("hdfs.nn.host")).isEqualTo("localhost");
    }

    @Test
    public void testInitialize() throws Exception {
        AssignHostsAction assignHostsAction = new AssignHostsAction();
        assignHostsAction.initialize(null);
    }
}