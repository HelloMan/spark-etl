package etl.installer.panel.validation;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.api.exception.IzPackException;
import etl.installer.panel.VariableConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseConnectionValidator extends AbstractDataValidator {
    private static final Logger logger = Logger.getLogger(DatabaseConnectionValidator.class.getName());

    @Override
    public Status validateData(InstallData installData) {
        try {
            Class.forName(getDriver(installData));
        } catch (ClassNotFoundException ex) {
            throw new IzPackException(ex);
        }

        try {
            validateJdbc(
                    installData.getVariable(VariableConstants.IGNIS_JDBC_URL),
                    installData.getVariable(VariableConstants.IGNIS_JDBC_USERNAME),
                    installData.getVariable(VariableConstants.IGNIS_JDBC_PASSWORD));

            validateJdbc(
                    installData.getVariable(VariableConstants.IGNIS_JDBC_URL),
                    installData.getVariable(VariableConstants.IGNIS_APP_JDBC_USERNAME),
                    installData.getVariable(VariableConstants.IGNIS_APP_JDBC_PASSWORD));
        } catch (SQLException ex) {
            errorMessage = "Could not connect to database: \n\n" + ex.getLocalizedMessage();
            logger.log(Level.SEVERE, errorMessage, ex);
            return Status.ERROR;
        }

        return Status.OK;
    }

    private void validateJdbc(String url, String username, String password) throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }


    public String getDriver(InstallData installData) {
        return installData.getVariable(VariableConstants.IGNIS_JDBC_DRIVER);
    }



}
