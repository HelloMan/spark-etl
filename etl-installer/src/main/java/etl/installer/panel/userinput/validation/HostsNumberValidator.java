package etl.installer.panel.userinput.validation;

import com.izforge.izpack.panels.userinput.processorclient.ProcessingClient;
import com.izforge.izpack.panels.userinput.validator.Validator;

import java.util.logging.Logger;

public class HostsNumberValidator implements Validator {
    private static final Logger logger = Logger.getLogger(HostsNumberValidator.class.getName());


    @Override
    public boolean validate(ProcessingClient client) {
        String count = client.getConfigurationOptionValue("minHostNum");
        return  count != null && getUserInputHostsNumber(client) >= Integer.parseInt(count);
    }

    private int getUserInputHostsNumber(ProcessingClient client) {
        return client.getText().split(",").length;
    }


}
