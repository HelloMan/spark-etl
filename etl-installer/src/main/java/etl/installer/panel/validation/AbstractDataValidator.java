package etl.installer.panel.validation;

import com.izforge.izpack.api.installer.DataValidator;

public abstract class AbstractDataValidator implements DataValidator {

	protected String warnMessage = "validate failed";
	protected String errorMessage = "validate failed";

    @Override
    public String getErrorMessageId() {
        return  errorMessage;
    }


    @Override
    public String getWarningMessageId() {
        return warnMessage;
    }

    @Override
    public boolean getDefaultAnswer() {
        return false;
    }

}
