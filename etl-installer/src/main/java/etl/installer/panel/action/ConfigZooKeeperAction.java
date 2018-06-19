package etl.installer.panel.action;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.api.data.PanelActionConfiguration;
import com.izforge.izpack.api.handler.AbstractUIHandler;
import com.izforge.izpack.data.PanelAction;
import etl.installer.core.InstallerHelper;
import etl.installer.core.IpAddress;
import etl.installer.panel.VariableConstants;
import com.machinezoo.noexception.Exceptions;
import one.util.streamex.EntryStream;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ConfigZooKeeperAction implements PanelAction {

    @Override
    public void executeAction(InstallData adata, AbstractUIHandler handler) {
        adata.setVariable(VariableConstants.ZOOKEEPER_MYID, "");

        InstallerHelper installerHelper = new InstallerHelper(adata);
        if (installerHelper.isDistributed()) {
            getZookeeperId(adata).ifPresent(myId -> adata.setVariable(VariableConstants.ZOOKEEPER_MYID, String.valueOf(myId)));
        }

    }
    private Optional<Integer> getZookeeperId(InstallData adata) {
        Predicate<Map.Entry<Integer, String>> predicate = Exceptions.sneak().predicate(
                entry -> IpAddress.isThisMyIpAddress(InetAddress.getByName(entry.getValue())));
        return EntryStream.of(sortedZookeeperHosts(adata))
                .findFirst(predicate)
                .map(entry -> entry.getKey() + 1);


    }

    private List<String> sortedZookeeperHosts(InstallData adata) {
        return Arrays.stream(adata.getVariable(VariableConstants.ZOOKEEPER_HOSTS).split(",")).sorted().collect(Collectors.toList());
    }

    @Override
    public void initialize(PanelActionConfiguration configuration) {

    }
}
