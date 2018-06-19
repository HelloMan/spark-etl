package etl.installer.core;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.installer.bootstrap.Installer;
import etl.installer.panel.VariableConstants;

public class InstallerHelper {


    private final InstallData installData;

    public InstallerHelper(InstallData installData) {
        this.installData = installData;
    }

    public static  boolean isAutoInstall() {
        return Installer.getInstallerMode() == Installer.INSTALLER_AUTO;
    }
    public static boolean useDefaultsFile() {
        return "true".equalsIgnoreCase(System.getProperty("useDefaultsFile"));
    }

    public boolean isDistributed() {
        return "true".equalsIgnoreCase(installData.getVariable(VariableConstants.DISTRIBUTED));
    }


}
