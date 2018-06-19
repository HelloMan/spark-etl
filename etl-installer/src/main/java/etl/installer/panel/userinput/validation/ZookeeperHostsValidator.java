package etl.installer.panel.userinput.validation;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.panels.userinput.processorclient.ProcessingClient;
import com.izforge.izpack.panels.userinput.validator.Validator;
import etl.installer.panel.VariableConstants;

import java.util.Arrays;

public class ZookeeperHostsValidator implements Validator {

    private final InstallData installData;

    public ZookeeperHostsValidator(InstallData installData) {
        this.installData = installData;
    }


    @Override
    public boolean validate(ProcessingClient client) {
        if (Boolean.valueOf(installData.getVariable(VariableConstants.DISTRIBUTED))) {
            long zookeeperHostsNumber = Arrays.stream(client.getText().split(",")).count();
            return !(zookeeperHostsNumber < 3 || zookeeperHostsNumber % 2 == 0);
        }
        return true;
    }


}
