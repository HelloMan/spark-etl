package etl.installer.panel.action;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.api.data.PanelActionConfiguration;
import com.izforge.izpack.api.handler.AbstractUIHandler;
import com.izforge.izpack.data.PanelAction;
import etl.installer.core.Cluster;
import etl.installer.core.Component;
import etl.installer.core.InstallerHelper;
import etl.installer.core.Node;
import etl.installer.panel.VariableConstants;
import one.util.streamex.IntStreamEx;

import java.util.logging.Level;
import java.util.logging.Logger;

public class AssignHostsAction implements PanelAction {

    private static final Logger logger = Logger.getLogger(LoadServerIpAction.class.getName());

    @Override
    public void executeAction(InstallData adata, AbstractUIHandler handler) {
        InstallerHelper installerHelper = new InstallerHelper(adata);
        if (installerHelper.isDistributed()) {
            logger.log(Level.INFO, "hosts are: {0}", adata.getVariable(VariableConstants.HOSTS));
            setHostsVariable(adata);
            if (InstallerHelper.isAutoInstall()) {
                //don't need to set default for silent install mode.
                return;
            }
            Cluster cluster = new Cluster(adata.getVariable(VariableConstants.HOSTS).split(","));
            adata.setVariable(VariableConstants.HDFS_NN_HOST, cluster.getHost(Component.NAMENODE));
            adata.setVariable(VariableConstants.HDFS_SN_HOST, cluster.getHost(Component.SECONDARY_NAMENODE));
            adata.setVariable(VariableConstants.YARN_RM_HOST, cluster.getHost(Component.RESOURCE_MANAGER));
            adata.setVariable(VariableConstants.HISTORY_SERVER_HOST, cluster.getHost(Component.HISTORY_SERVER));
            adata.setVariable(VariableConstants.HBASE_MASTER_HOST, cluster.getHost(Component.HMASTER));
            adata.setVariable(VariableConstants.HBASE_BACKUP_MASTER_HOSTS, cluster.getHost(Component.HMASTER_BACKUP));
            adata.setVariable(VariableConstants.ZOOKEEPER_HOSTS, String.join(",", cluster.getHosts(Component.ZOOKEEPER)));
            adata.setVariable(VariableConstants.HDFS_DN_HOSTS, String.join(",", cluster.getHosts(Component.DATANODE)));
            adata.setVariable(VariableConstants.YARN_NM_HOSTS, String.join(",", cluster.getHosts(Component.NODE_MANAGER)));
            adata.setVariable(VariableConstants.HBASE_REGIONSERVER_HOSTS, String.join(",", cluster.getHosts(Component.REGION_SERVER)));
            adata.setVariable(VariableConstants.PHOENIX_QS_HOSTS, String.join(",", cluster.getHosts(Component.QUERY_SERVER)));
            adata.setVariable(VariableConstants.IGNIS_HOST, cluster.getHost(Component.IGNIS));
        } else {
            adata.setVariable(VariableConstants.HDFS_NN_HOST, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HDFS_SN_HOST, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.YARN_RM_HOST, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HISTORY_SERVER_HOST, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HBASE_MASTER_HOST, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HBASE_BACKUP_MASTER_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.ZOOKEEPER_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HDFS_DN_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.YARN_NM_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.HBASE_REGIONSERVER_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.PHOENIX_QS_HOSTS, Node.LOCALHOST.getHost());
            adata.setVariable(VariableConstants.IGNIS_HOST, Node.LOCALHOST.getHost());

        }

    }

    private void setHostsVariable(InstallData adata) {
        if (!InstallerHelper.isAutoInstall()) {
            int hostCount = Integer.parseInt(adata.getVariable(VariableConstants.HOSTS_COUNT));
            String hosts = IntStreamEx
                    .range(1, hostCount + 1)
                    .mapToObj(i -> adata.getVariable(VariableConstants.HOSTS_PREFIX + i))
                    .joining(",");

            adata.setVariable(VariableConstants.HOSTS, hosts);
        }
    }

    @Override
    public void initialize(PanelActionConfiguration configuration) {

    }
}
