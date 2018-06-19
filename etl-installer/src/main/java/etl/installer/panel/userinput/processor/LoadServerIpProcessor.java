package etl.installer.panel.userinput.processor;


import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.panels.userinput.processor.Processor;
import com.izforge.izpack.panels.userinput.processorclient.ProcessingClient;
import etl.installer.core.InstallerHelper;
import etl.installer.panel.VariableConstants;

import java.util.ArrayList;
import java.util.List;


public class LoadServerIpProcessor implements Processor {

    private InstallData installData;

    public LoadServerIpProcessor(InstallData installData) {
        this.installData = installData;
    }

    public String process(ProcessingClient client) {
        InstallerHelper installerHelper = new InstallerHelper(installData);
        if (!installerHelper.isDistributed()) {
            return "localhost";
        }
        int hostCount = Integer.parseInt(installData.getVariable(VariableConstants.HOSTS_COUNT));
        List<String> hostVars = new ArrayList<>();
        for (int j = 1; j <= hostCount; j++) {
            String var = VariableConstants.HOSTS_PREFIX + j;
            if (installData.getVariable(var) != null) {
                hostVars.add(installData.getVariable(var));
            }
        }
        return String.join(":", hostVars);

    }

}
