package etl.installer.panel.userinput.validation;

import com.izforge.izpack.api.exception.IzPackException;
import com.izforge.izpack.panels.userinput.processorclient.ProcessingClient;
import com.izforge.izpack.panels.userinput.validator.Validator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HostsAddressValidator  implements Validator {
    private static final Logger logger = Logger.getLogger(HostsAddressValidator.class.getName());

    @Override
    public boolean validate(ProcessingClient client) {
        return Arrays.stream(client.getText().split(",")).anyMatch(this::testHost);
    }

    public boolean testHost(String host) {
        try {
            if (isLocalHost(host)) {
                logger.log(Level.SEVERE, "host {0} is 127.0.0.1 or localhost which is not allowed in distributed mode", host);
                return true;
            }
            InetAddress.getByName(host);
            return true;
        } catch (UnknownHostException e) {
            throw new IzPackException(e);
        }
    }

    private boolean isLocalHost(String host) {
        return "127.0.0.1".equalsIgnoreCase(host) || "localhost".equalsIgnoreCase(host);
    }
}
