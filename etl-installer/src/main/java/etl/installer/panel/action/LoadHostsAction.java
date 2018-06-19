package etl.installer.panel.action;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.api.data.PanelActionConfiguration;
import com.izforge.izpack.api.exception.IzPackException;
import com.izforge.izpack.api.handler.AbstractUIHandler;
import com.izforge.izpack.data.PanelAction;
import etl.installer.core.InstallerHelper;
import etl.installer.panel.VariableConstants;
import one.util.streamex.EntryStream;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadHostsAction implements PanelAction {

    @Override
    public void executeAction(InstallData installData, AbstractUIHandler abstractUIHandler) {
        if (InstallerHelper.useDefaultsFile()) {
            String hosts = installData.getVariable(VariableConstants.HOSTS);
            if (StringUtils.isNotEmpty(hosts)) {
                List<String> hostList = Arrays.stream(hosts.split(",")).sorted().collect(Collectors.toList());
                EntryStream.of(hostList).forEach(entry -> setHostVairable(entry, installData));
                installData.setVariable(VariableConstants.HOSTS_COUNT, String.valueOf(hostList.size()));
            }else {
                throw new IzPackException("hosts property can not be empty");
            }
        }
    }

    private void setHostVairable(Map.Entry<Integer,String> entry, InstallData installData) {
        int hostIdx = entry.getKey() +1;
        installData.setVariable(VariableConstants.HOSTS_PREFIX + hostIdx, entry.getValue());
    }


    @Override
    public void initialize(PanelActionConfiguration panelActionConfiguration) {

    }


}
